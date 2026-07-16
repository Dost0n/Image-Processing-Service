# worker.py
import asyncio
import json
import logging
from pathlib import Path
import aio_pika
from PIL import Image, UnidentifiedImageError
from sqlalchemy import select
from session import AsyncSessionLocal
from models import Task
from config import settings
import signal
import time
from metrics import tasks_total, task_duration, tasks_in_progress
from prometheus_client import start_http_server


from tenacity import (
    retry, stop_after_attempt, wait_exponential,
    retry_if_exception_type, RetryError
)
from queues import setup_queues

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)
shutdown_event = asyncio.Event()

UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)


SIZES = {
    "thumb": 150,
    "medium": 600,
    "large": 1200,
}

class TransientError(Exception):
    """Vaqtinchalik xato — qayta urinsa bo'ladi."""


class PermanentError(Exception):
    """Doimiy — darhol DLQ, retry behuda."""

# @retry(
#     stop=stop_after_attempt(3),
#     wait=wait_exponential(multiplier=1, min=1, max=6),
#     retry=retry_if_exception_type(TransientError), 
#     reraise=True,
# )
async def _process_with_retry(task_id: str, file_path: Path) -> dict:
    loop = asyncio.get_running_loop()
    try:
        sizes = await loop.run_in_executor(None, process_image, file_path)
        return sizes
    except FileNotFoundError as e:
        raise PermanentError(f"Fayl topilmadi: {e}") from e
    except ConnectionError as e:
        raise TransientError(f"Ulanish uzildi: {e}") from e
    except (UnidentifiedImageError, OSError) as e:
        raise PermanentError(f"Rasm buzuq: {e}") from e
    except Exception as e:
        raise TransientError(str(e)) from e


def process_image(file_path: Path) -> dict:
    results = {}
    with Image.open(file_path) as img:
        if img.mode in ("RGBA", "LA", "P"):
            background = Image.new("RGB", img.size, (255, 255, 255))

            rgb_img = img.convert("RGBA")
            background.paste(rgb_img, mask=rgb_img.split()[-1])
            img = background
        elif img.mode != "RGB":
            img = img.convert("RGB")

        for name, width in SIZES.items():
            resized = img.copy()
            resized.thumbnail((width, width), Image.Resampling.LANCZOS)
            output_file = UPLOAD_DIR / f"{file_path.stem}_{name}.jpg"
            resized.save(output_file, format="JPEG", quality=90, optimize=True)
            results[name] = str(output_file)
    return results


async def handle_task(payload: dict):
    task_id = payload["task_id"]
    file_path = Path(payload["file_path"])

    async with AsyncSessionLocal() as db:
        result = await db.execute(select(Task).where(Task.task_id == task_id))
        task = result.scalar_one_or_none()
        if task is None:
            logger.warning(f"[topilmadi] {task_id}")
            return
        if task.status == "done":
            logger.info(f"[o'tkazildi: allaqachon done] {task_id}")
            return
        task.status = "processing"
        await db.commit()
    
    tasks_in_progress.inc()
    start = time.perf_counter()
    try:
        logger.info(f"[boshlandi] {task_id}")
        sizes = await _process_with_retry(task_id, file_path)

        tasks_total.labels(status="done").inc()
    finally:
        tasks_in_progress.dec()
        task_duration.observe(time.perf_counter() - start) 

    async with AsyncSessionLocal() as db:
        result = await db.execute(select(Task).where(Task.task_id == task_id))
        task = result.scalar_one_or_none()
        task.status = "done"
        task.sizes = sizes
        await db.commit()

    logger.info(f"[tayyor] {task_id}")


async def mark_failed(task_id: str, error: str):
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(Task).where(Task.task_id == task_id))
        task = result.scalar_one_or_none()
        if task:
            task.status = "failed"
            task.error = error
            await db.commit()


async def mark_status(task_id: str, status: str):
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(Task).where(Task.task_id == task_id))
        task = result.scalar_one_or_none()
        if task:
            task.status = status
            await db.commit()


async def publish_to(channel, exchange_name, routing_key, body, retry_count):
    exchange = await channel.get_exchange(exchange_name)
    message = aio_pika.Message(
        body=body,
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        headers={"x-retry-count": retry_count},
    )
    await exchange.publish(message, routing_key=routing_key)


_shutdown = False

def _handle_signal(signum, frame):
    logger.info("To'xtash signali keldi...")
    try:
        asyncio.get_running_loop().call_soon_threadsafe(shutdown_event.set)
    except RuntimeError:
        shutdown_event.set()


async def main():
    start_http_server(8001)
    logger.info("Metrics server: http://localhost:8001/metrics")
    connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)
    queue = await setup_queues(channel)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    logger.info("Worker ishga tushdi...")

    async with queue.iterator() as messages:
        while not shutdown_event.is_set():
            next_msg = asyncio.ensure_future(messages.__anext__())
            stop = asyncio.ensure_future(shutdown_event.wait())
            done, pending = await asyncio.wait(
                {next_msg, stop}, return_when=asyncio.FIRST_COMPLETED
            )
            for t in pending:
                t.cancel()
            if stop in done:
                next_msg.cancel()
                break
            message = next_msg.result()
            retry_count = message.headers.get("x-retry-count", 0) if message.headers else 0
            payload = json.loads(message.body.decode())
            task_id = payload["task_id"]
            try:
                await handle_task(payload)
                await message.ack()
            except PermanentError as e:
                await message.ack()
                await mark_failed(task_id, str(e))
                logger.error(f"[DLQ: doimiy xato] {e}")
                await publish_to(channel, settings.DLQ_EXCHANGE, settings.QUEUE_NAME, message.body, retry_count)
            except Exception as e:
                await message.ack()
                if retry_count < settings.MAX_RETRIES:
                    await mark_status(task_id, "retrying")
                    logger.warning(f"[retry {retry_count + 1}/{settings.MAX_RETRIES}] {e}")
                    await publish_to(channel, settings.RETRY_EXCHANGE, settings.QUEUE_NAME, message.body, retry_count + 1)
                    tasks_total.labels(status="retried").inc()
                else:
                    await mark_failed(task_id, str(e))
                    logger.error(f"[DLQ: {settings.MAX_RETRIES} urinish tugadi] {e}")
                    await publish_to(channel, settings.DLQ_EXCHANGE, settings.QUEUE_NAME, message.body, retry_count)
                    tasks_total.labels(status="failed").inc()

    await connection.close()
    logger.info("Worker toza to'xtadi.")
      

if __name__ == "__main__":
    asyncio.run(main())



