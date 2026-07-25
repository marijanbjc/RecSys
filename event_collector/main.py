import asyncio
import json
import time

import aio_pika
import redis
from aio_pika import Message
from aio_pika.abc import AbstractRobustConnection, AbstractRobustExchange
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from shared.config import AppSettings, settings
from shared.logger import setup_logger
from shared.models import InteractEvent

logger = setup_logger("event-collector")
logger.setLevel(settings.LOG_LEVEL)
app = FastAPI()
# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class InteractionManager:
    def __init__(self, settings: AppSettings) -> None:
        self.interaction_prefix = settings.redis_settings.INTERACTION_PREFIX
        self.redis_connection = redis.Redis(
            host=settings.redis_settings.HOST,
            port=settings.redis_settings.PORT,
            db=settings.redis_settings.DB,
        )

    def add(self, user_id: str, items_ids: list[str]) -> None:
        try:
            logger.info(f"Finding user interaction-history {user_id}")
            history = self.redis_connection.json().get(
                f"{self.interaction_prefix}-{user_id}"
            )
            logger.info(f"Found history {history}")
            if history is None:
                history = []
            history.extend(items_ids)
            logger.info(f"Setting history {history}")
            self.redis_connection.json().set(
                f"{self.interaction_prefix}-{user_id}", ".", history
            )
        except Exception as e:
            logger.error(e)


interaction_manager = InteractionManager(settings)

_rabbitmq_connection: AbstractRobustConnection = None
_rabbitmq_exchange = None


async def create_rabbitmq_exchange(settings: AppSettings) -> AbstractRobustExchange:
    global _rabbitmq_exchange, _rabbitmq_connection
    if _rabbitmq_exchange is None:
        _rabbitmq_connection = await aio_pika.connect_robust(
            f"amqp://{settings.rabbit_settings.USER}:{settings.rabbit_settings.PASSWORD}@{settings.rabbit_settings.HOST}:{settings.rabbit_settings.PORT}/",
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
        await queue.bind(_rabbitmq_exchange, settings.rabbit_settings.ROUTING_KEY)
    return _rabbitmq_exchange


async def publish_message(settings: AppSettings, message: Message):
    rabbitmq_exchange = await create_rabbitmq_exchange(settings)
    await rabbitmq_exchange.publish(
        message,
        settings.rabbit_settings.ROUTING_KEY,
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

    interaction_manager.add(message.user_id, message.item_ids)
    return 200
