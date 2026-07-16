import os
from dotenv import load_dotenv

from pathlib import Path


env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)


class Settings:
    RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"
    RETRY_TTL = 5000

    QUEUE_NAME = "image_tasks_v3"
    RETRY_QUEUE = "image_retry_v3"
    DLQ_NAME = "image_dlq_v3"
    MAIN_EXCHANGE = "image_main_exchange"
    RETRY_EXCHANGE = "image_retry_exchange"
    DLQ_EXCHANGE = "image_dlq_exchange"
    MAX_RETRIES = 3

    DATABASE_URL = "sqlite+aiosqlite:///./tasks.db"
    
settings = Settings()
