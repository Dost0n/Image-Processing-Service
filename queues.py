import aio_pika

RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"

QUEUE_NAME = "image_tasks_v3"
RETRY_QUEUE = "image_retry_v3"
DLQ_NAME = "image_dlq_v3"
MAIN_EXCHANGE = "image_main_exchange"
RETRY_EXCHANGE = "image_retry_exchange"
DLQ_EXCHANGE = "image_dlq_exchange"
RETRY_TTL = 5000
MAX_RETRIES = 3


async def setup_queues(channel):
    main_ex = await channel.declare_exchange(MAIN_EXCHANGE, type="direct", durable=True)
    retry_ex = await channel.declare_exchange(RETRY_EXCHANGE, type="direct", durable=True)
    dlq_ex = await channel.declare_exchange(DLQ_EXCHANGE, type="direct", durable=True)

    main_q = await channel.declare_queue(QUEUE_NAME, durable=True)
    await main_q.bind(main_ex, routing_key=QUEUE_NAME)

    retry_q = await channel.declare_queue(
        RETRY_QUEUE,
        durable=True,
        arguments={
            "x-message-ttl": RETRY_TTL,
            "x-dead-letter-exchange": MAIN_EXCHANGE,
            "x-dead-letter-routing-key": QUEUE_NAME,
        },
    )
    await retry_q.bind(retry_ex, routing_key=QUEUE_NAME)

    dlq_q = await channel.declare_queue(DLQ_NAME, durable=True)
    await dlq_q.bind(dlq_ex, routing_key=QUEUE_NAME)

    return main_q