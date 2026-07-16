from fastapi import UploadFile, File, HTTPException, APIRouter
from pathlib import Path
from uuid import uuid4
from messaging import publish_task
from sqlalchemy import select
from session import AsyncSessionLocal, create_tables
from models import Task


api_router = APIRouter()


UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)


ALLOWED_TYPES = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
}

@api_router.post("/images")
async def upload_image(file: UploadFile = File(...)):
    if file.content_type not in ALLOWED_TYPES:
        raise HTTPException(
            status_code=400,
            detail="Faqat JPG, PNG va WEBP rasmlar ruxsat etiladi."
        )
    
    extension = ALLOWED_TYPES[file.content_type]
    filename = f"{uuid4().hex}{extension}"
    file_path = UPLOAD_DIR / filename

    with open(file_path, "wb") as buffer:
        while chunk := await file.read(1024 * 1024):
            buffer.write(chunk)
    
    
    task_id = uuid4().hex

    async with AsyncSessionLocal() as db:
        task = Task(
            task_id=task_id,
            status="pending",
            original_path=str(file_path),
        )
        db.add(task)
        await db.commit()
    await publish_task(task_id, str(file_path))
    return {"task_id": task_id, "status": "pending"}


@api_router.get("/images/{task_id}")
async def get_task(task_id: str):
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(Task).where(Task.task_id == task_id))
        task = result.scalar_one_or_none()
        if task is None:
            raise HTTPException(status_code=404, detail="Bunday task topilmadi")
        return task