import asyncio
import json
import time

import aio_pika
from aio_pika import Message
from aio_pika.abc import AbstractRobustConnection, AbstractRobustExchange
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from shared.config import AppSettings, settings
from shared.models import InteractEvent
from shared.watched_filter import WatchedFilter

app = FastAPI()
# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


watched_filter = WatchedFilter(settings)

_rabbitmq_connection: AbstractRobustConnection = None
_rabbitmq_exchange = None


async def create_rabbitmq_exchange(settings: AppSettings) -> AbstractRobustExchange:
    global _rabbitmq_exchange, _rabbitmq_connection
    if _rabbitmq_exchange is None:
        _rabbitmq_connection = await aio_pika.connect_robust(
            f"amqp://{settings.rabbit_settings.HOST}:{settings.rabbit_settings.USER}@rabbitmq:{settings.rabbit_settings.PORT}/",
            loop=asyncio.get_event_loop(),
        )

        # Creating channel
        channel = await _rabbitmq_connection.channel()

        # Declaring exchange
        _rabbitmq_exchange = await channel.declare_exchange(
            settings.rabbit_settings.EXCHANGE, type="direct"
        )

        # Declaring queue
        queue = await channel.declare_queue(settings.rabbit_settings.QUEUE_NAME)

        # Binding queue
        await queue.bind(_rabbitmq_exchange, settings.rabbit_settings.routing_key)
    return _rabbitmq_exchange


async def publish_message(settings: AppSettings, message: Message):
    rabbitmq_exchange = await create_rabbitmq_exchange(settings)
    await rabbitmq_exchange.publish(
        message,
        settings.rabbit_settings.routing_key,
    )


@app.get("/healthcheck")
def read_root():
    return True


@app.post("/interact")
async def interact(message: InteractEvent) -> int:
    message.timestamp = time.time()
    await publish_message(
        message=Message(
            bytes(json.dumps(message.model_dump()), "utf-8"),
            content_type="text/json",
        ),
        settings=settings,
    )

    watched_filter.add(message.user_id, message.item_ids)
    return 200
