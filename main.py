from fastapi import FastAPI
import uvicorn
from contextlib import asynccontextmanager
import messaging
from router import api_router
from session import create_tables


def include_router(app):
    app.include_router(api_router, prefix="")



@asynccontextmanager
async def lifespan(app: FastAPI):
    await create_tables()  
    await messaging.connect()
    yield
    await messaging.close()


def start_application():
    app = FastAPI(lifespan=lifespan)
    include_router(app)
    return app


app = start_application()

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

