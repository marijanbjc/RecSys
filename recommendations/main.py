import random

import numpy as np
import redis
from fastapi import FastAPI

from shared.config import AppSettings, settings
from shared.logger import setup_logger
from shared.models import NewItemsEvent, RecommendationsResponse
from shared.watched_filter import WatchedFilter

logger = setup_logger("recommendation_logger")
logger.setLevel(settings.LOG_LEVEL)


unique_item_ids = set()


class RecommendationManager:
    def __init__(self, settings: AppSettings):
        logger.info("Initializing recommendation manager")
        self.INTERACTION_PREFIX = settings.redis_settings.INTERACTION_PREFIX
        self.TOP_RECOMMENDATION_PREFIX = (
            settings.redis_settings.TOP_RECOMMENDATION_PREFIX
        )
        self.ALS_RECOMMENDATION_PREFIX = (
            settings.redis_settings.ALS_RECOMMENDATION_PREFIX
        )

        self.TOP_K = settings.recommendation_settings.TOP_K
        self.EPSILON = settings.recommendation_settings.EPSILON

        self.redis_connection = redis.Redis(
            host=settings.redis_settings.HOST,
            port=settings.redis_settings.PORT,
            db=settings.redis_settings.DB,
        )

        self.watched_filter = WatchedFilter(settings)

    def clear(self) -> None:
        global unique_item_ids
        unique_item_ids = set()

        self.redis_connection.json().delete(f"{self.INTERACTION_PREFIX}*")
        self.redis_connection.json().delete(f"{self.TOP_RECOMMENDATION_PREFIX}*")
        self.redis_connection.json().delete(f"{self.ALS_RECOMMENDATION_PREFIX}*")

    @staticmethod
    def add_items(item_ids: list[str]) -> None:
        global unique_item_ids

        for item_id in item_ids:
            unique_item_ids.add(item_id)

    def add_random_items(self, item_ids: list[str]) -> list[str]:
        global unique_item_ids
        if (
            len(item_ids) == 0
            or random.random() < self.EPSILON
            and len(unique_item_ids) > 0
        ):
            random_ids = np.random.choice(
                list(unique_item_ids), size=max(17 - len(item_ids), 2), replace=False
            ).tolist()
            item_ids += random_ids
        return item_ids

    def get_user_recommendations(self, user_id: str) -> list[str]:
        try:
            logger.info(f"Getting recommendations for user {user_id}")

            user_history = self.redis_connection.keys(
                f"{self.INTERACTION_PREFIX}-{user_id}*"
            )

            if len(user_history) == 0:
                logger.info(f"No history for {user_id}")
                logger.info(f"Getting for {user_id} top-items")

                top_items = self.redis_connection.json().get(
                    self.TOP_RECOMMENDATION_PREFIX
                )

                item_ids = np.random.choice(top_items, self.TOP_K, replace=False)
                logger.info(f"Sampled top items: {item_ids}")

            else:
                logger.info(f"History for {user_id} has {len(user_history)} items")

                logger.info(f"Getting for {user_id} als-items")
                item_ids = self.redis_connection.json().get(
                    f"{self.ALS_RECOMMENDATION_PREFIX}-{user_id}"
                )

                if item_ids is not None:
                    logger.info(
                        f"Als-items for {user_id} has {len(item_ids)} items, filtering watched"
                    )
                    item_ids = self.watched_filter.filter_user_items(user_id, item_ids)
                    item_ids = np.random.choice(item_ids, self.TOP_K, replace=False)
                else:
                    logger.info(f"{user_id} has no als-items")
                    item_ids = []

                logger.info(
                    f"Recommendations len als-items for {user_id} after filtering={len(item_ids)}"
                )

        except Exception as e:
            logger.error(f"Exception while getting recommendations for {user_id}: {e}")
            item_ids = []

        logger.info(f"Adding random items recommendations for {user_id} to exploration")
        item_ids = self.add_random_items(item_ids)

        logger.info(f"Final recommendations for {user_id}: {item_ids}")

        return item_ids

app = FastAPI()
recommendation_manager = RecommendationManager(settings)


@app.get("/healthcheck")
def healthcheck() -> int:
    return 200


@app.get("/cleanup")
def cleanup():
    recommendation_manager.clear()
    return 200


@app.post("/add_items")
def add_items(request: NewItemsEvent):

    recommendation_manager.add_items(request.item_ids)

    return 200


@app.get("/recs/{user_id}")
def get_recs(user_id: str):
    recommendations = recommendation_manager.get_user_recommendations(user_id)

    return RecommendationsResponse(item_ids=recommendations)
