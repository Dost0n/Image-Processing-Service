import aio_pika
from config import settings

async def setup_queues(channel):
    main_ex = await channel.declare_exchange(settings.MAIN_EXCHANGE, type="direct", durable=True)
    retry_ex = await channel.declare_exchange(settings.RETRY_EXCHANGE, type="direct", durable=True)
    dlq_ex = await channel.declare_exchange(settings.DLQ_EXCHANGE, type="direct", durable=True)

    main_q = await channel.declare_queue(settings.QUEUE_NAME, durable=True)
    await main_q.bind(main_ex, routing_key=settings.QUEUE_NAME)

    retry_q = await channel.declare_queue(
        settings.RETRY_QUEUE,
        durable=True,
        arguments={
            "x-message-ttl": settings.RETRY_TTL,
            "x-dead-letter-exchange": settings.MAIN_EXCHANGE,
            "x-dead-letter-routing-key": settings.QUEUE_NAME,
        },
    )
    await retry_q.bind(retry_ex, routing_key=settings.QUEUE_NAME)

    dlq_q = await channel.declare_queue(settings.DLQ_NAME, durable=True)
    await dlq_q.bind(dlq_ex, routing_key=settings.QUEUE_NAME)

    return main_q