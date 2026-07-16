import json
import aio_pika
from aio_pika import Message, DeliveryMode
from queues import setup_queues
from config import settings


_connection = None
_channel = None


async def connect():
    global _connection, _channel
    _connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)
    _channel = await _connection.channel()
    await setup_queues(_channel)


async def publish_task(task_id: str, file_path: str):
    payload = {"task_id": task_id, "file_path": file_path}
    message = Message(
        body=json.dumps(payload).encode(),
        delivery_mode=DeliveryMode.PERSISTENT,
        headers={"x-retry-count": 0},
    )
    exchange = await _channel.get_exchange(settings.MAIN_EXCHANGE)
    await exchange.publish(message, routing_key=settings.QUEUE_NAME)


async def close():
    if _connection:
        await _connection.close()