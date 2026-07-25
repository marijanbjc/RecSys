import asyncio
import json
import os.path
import time
from functools import partial

import aio_pika
import numpy as np
import optuna
import polars as pl
import redis
import scipy.sparse as sp
from implicit.als import AlternatingLeastSquares
from pydantic import BaseModel
from shared.config import AppSettings, RedisSettings, RegularPipelineSettings, settings
from shared.logger import setup_logger

logger = setup_logger("regular_pipeline")
logger.setLevel(settings.LOG_LEVEL)


class RedisManager:
    def __init__(self, settings: RedisSettings):
        self.redis_connection = redis.Redis(
            host=settings.HOST,
            port=settings.PORT,
            db=settings.DB,
        )

        self.TOP_RECOMMENDATION_PREFIX = settings.TOP_RECOMMENDATION_PREFIX

        self.ALS_RECOMMENDATION_PREFIX = settings.ALS_RECOMMENDATION_PREFIX

    def set_top_items(self, top_items: list[str]) -> None:
        self.redis_connection.json().set(self.TOP_RECOMMENDATION_PREFIX, ".", top_items)

    def set_user_als_items(self, user_id: str, items: list[str]) -> None:
        self.redis_connection.json().set(
            f"{self.ALS_RECOMMENDATION_PREFIX}-{user_id}", ".", items
        )

    def close(self) -> None:
        self.redis_connection.close()


async def collect_messages(settings: AppSettings):
    connection = await aio_pika.connect_robust(
        f"amqp://{settings.rabbit_settings.USER}:{settings.rabbit_settings.PASSWORD}@{settings.rabbit_settings.HOST}:{settings.rabbit_settings.PORT}/",
        loop=asyncio.get_event_loop(),
    )

    async with connection:
        # Creating channel
        channel = await connection.channel()

        # Will take no more than 10 messages in advance
        await channel.set_qos(prefetch_count=10)

        # Declaring queue
        queue = await channel.declare_queue(settings.rabbit_settings.QUEUE_NAME)

        # Declaring exchange
        exchange = await channel.declare_exchange(
            settings.rabbit_settings.EXCHANGE, type="direct"
        )
        await queue.bind(exchange, settings.rabbit_settings.ROUTING_KEY)

        t_start = time.time()
        data = []
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    message = message.body.decode()
                    if (
                        time.time() - t_start
                        > settings.regular_pipeline_settings.MESSAGES_COLLECTION_INTERVAL
                    ):
                        logger.info("Saving events from rabbitmq")
                        # update data if 10s passed
                        new_data = pl.DataFrame(data)

                        if len(new_data) > 0:
                            new_data = new_data.explode(["item_ids", "actions"]).rename(
                                {"item_ids": "item_id", "actions": "action"}
                            )

                            if os.path.exists("data/interactions.csv"):
                                data = pl.concat(
                                    [
                                        pl.read_csv(
                                            "data/interactions.csv",
                                            dtypes=[
                                                pl.Utf8,
                                                pl.Utf8,
                                                pl.Utf8,
                                                pl.Float64,
                                            ],
                                        ),
                                        new_data,
                                    ]
                                )
                            else:
                                data = new_data

                            data.write_csv("data/interactions.csv")

                        data = []
                        t_start = time.time()

                    logger.info(f"events from rabbitmq saved, {len(data)} rows")

                    message = json.loads(message)
                    data.append(message)


def calculate_top_recommendations(
    interactions: pl.DataFrame, redis_manager: RedisManager
) -> None:
    logger.info(f"unique items: {interactions['item_id'].n_unique()}")
    top_items = (
        interactions.sort("timestamp")
        .unique(["user_id", "item_id", "action"], keep="last")
        .filter(pl.col("action") == "like")
        .groupby("item_id")
        .count()
        .sort("count", descending=True)
        .head(100)
    )["item_id"].to_list()

    top_items = [str(item_id) for item_id in top_items]

    redis_manager.set_top_items(top_items)


class DataPreprocessor:
    def __init__(self, min_positive_actions: int = 1, test_size: int = 1):
        self.min_positive_actions = min_positive_actions
        self.test_size = test_size
        self.users_mapping: dict[int, int] | None = None
        self.items_mapping: dict[int, int] | None = None
        self.inverted_users_mapping: dict[int, int] | None = None
        self.inverted_items_mapping: dict[int, int] | None = None

    def prepare_mappings(self, interactions: pl.DataFrame) -> pl.DataFrame:
        users = interactions["user_id"].unique().to_list()
        items = interactions["item_id"].unique().to_list()

        self.users_mapping = {user_id: i for i, user_id in enumerate(users)}
        self.inverted_users_mapping = {
            i: user_id for user_id, i in self.users_mapping.items()
        }

        self.items_mapping = {item_id: i for i, item_id in enumerate(items)}
        self.inverted_items_mapping = {
            i: item_id for item_id, i in self.items_mapping.items()
        }

        return interactions.with_columns(
            [
                pl.col("user_id").map_dict(self.users_mapping),
                pl.col("item_id").map_dict(self.items_mapping),
            ]
        )

    def prepare_features(
        self, interactions: pl.DataFrame
    ) -> tuple[pl.DataFrame, sp.csr_matrix]:
        interactions = self.prepare_mappings(interactions)

        interactions_grouped = (
            interactions.with_columns(
                pl.col("action").apply(lambda x: 1 if x == "like" else -1)
            )
            .sort("timestamp")
            .groupby("user_id")
            .agg(
                [
                    pl.col("item_id").alias("train_item_ids"),
                    pl.col("action").alias("train_ratings"),
                ]
            )
            .filter(pl.col("train_ratings").apply(lambda x: any([i == 1 for i in x])))
        )

        if interactions_grouped.shape[0] == 0:
            return None, None

        user_item_data = self._create_sparse_matrix(
            interactions_grouped, "train_item_ids", "train_ratings"
        )
        return interactions_grouped, user_item_data

    def _create_sparse_matrix(
        self,
        interactions_grouped: pl.DataFrame,
        items_col: str = "item_ids",
        ratings_col: str = "ratings",
    ) -> sp.csr_matrix:
        rows, cols, data = [], [], []

        for user_id, item_ids, ratings in interactions_grouped.select(
            "user_id", items_col, ratings_col
        ).rows():
            rows.extend([user_id] * len(item_ids))
            cols.extend(item_ids)
            data.extend(ratings)

        return sp.csr_matrix((data, (rows, cols)), dtype=np.float32)


class ModelParams(BaseModel):
    factors: int
    iterations: int
    alpha: float
    regularization: float
    random_state: int


class ALSRecommender:
    def __init__(self, settings: RegularPipelineSettings):
        self.model_params = ModelParams(
            factors=settings.ALS_FACTORS,
            iterations=settings.ALS_ITERATIONS,
            alpha=settings.ALS_ALPHA,
            regularization=settings.ALS_REGULARIZATION,
            random_state=settings.ALS_RANDOM_STATE,
        )

        self.model: AlternatingLeastSquares | None = None

    def fit(self, user_item_data: sp.csr_matrix) -> None:
        self.model = AlternatingLeastSquares(**self.model_params.model_dump())
        self.model.fit(user_item_data)

    def recommend_all(
        self,
        user_item_data: sp.csr_matrix,
        interactions_grouped: pl.DataFrame,
        preprocessor: DataPreprocessor,
        redis_manager: RedisManager | None = None,
        k: int = 10,
        filter_seen: bool = True,
    ) -> dict[int, list[int]]:
        if self.model is None:
            raise ValueError("Model not trained. Call fit() first.")

        recs = self.model.recommend_all(
            user_item_data, k * 3
        )  # Get extra for filtering
        recommendations = {}

        for user_idx, user_history in interactions_grouped.select(
            "user_id", "train_item_ids"
        ).rows():
            user_id = preprocessor.inverted_users_mapping[user_idx]

            if filter_seen:
                user_recommendations = [
                    preprocessor.inverted_items_mapping[item_idx]
                    for item_idx in recs[user_idx]
                    if item_idx not in user_history
                ][
                    :k
                ]  # Take top k after filtering
            else:
                user_recommendations = [
                    preprocessor.inverted_items_mapping[item_idx]
                    for item_idx in recs[user_idx][:k]
                ]

            recommendations[user_id] = user_recommendations

            if redis_manager:
                redis_manager.set_user_als_items(
                    str(user_id), [str(item) for item in user_recommendations]
                )

        return recommendations


class ModelEvaluator:
    @staticmethod
    def user_hitrate(relevant_items: list, recommended_items: list, k: int = 10) -> int:
        return int(len(set(relevant_items).intersection(recommended_items[:k])) > 0)

    def evaluate(
        self,
        recommendations: dict[int, list[int]],
        interactions_grouped: pl.DataFrame,
        preprocessor: DataPreprocessor,
        k: int = 10,
    ) -> float:
        hitrates = []

        for user_idx, test_item_ids, test_ratings in interactions_grouped.select(
            "user_id", "test_item_ids", "test_ratings"
        ).rows():
            user_id = preprocessor.inverted_users_mapping[user_idx]

            # Get only relevant test items (where rating is positive)
            relevant_items = [
                preprocessor.inverted_items_mapping[test_item_ids[i]]
                for i, rating in enumerate(test_ratings)
                if rating == 1
            ]

            if len(relevant_items) == 0:
                continue

            user_recommendations = recommendations.get(user_id, [])

            hitrates.append(self.user_hitrate(relevant_items, user_recommendations, k))

        return np.mean(hitrates)


class HyperparameterOptimizer:
    def __init__(self, n_trials: int = 10):
        self.n_trials = n_trials

    def objective(
        self,
        trial: optuna.Trial,
        user_item_data: sp.csr_matrix,
        interactions_grouped: pl.DataFrame,
        preprocessor: DataPreprocessor,
        k: int = 10,
    ) -> float:
        params = {
            "factors": trial.suggest_int("factors", 8, 128),
            "iterations": trial.suggest_int("iterations", 5, 15),
            "alpha": trial.suggest_float("alpha", 0.1, 5.0),
            "regularization": trial.suggest_float("regularization", 1e-3, 1.0),
            "random_state": 42,
        }

        logger.info(f"Trial parameters: {params}")

        # Train model with suggested parameters
        recommender = ALSRecommender(ModelParams(**params))
        recommender.fit(user_item_data)

        # Get recommendations
        recommendations = recommender.recommend_all(
            user_item_data=user_item_data,
            interactions_grouped=interactions_grouped,
            preprocessor=preprocessor,
            k=k * 2,
        )

        # Evaluate model
        evaluator = ModelEvaluator()
        hitrate = evaluator.evaluate(
            recommendations, interactions_grouped, preprocessor, k
        )

        logger.info(f"Hitrate@{k} = {hitrate}")

        return hitrate

    def optimize(
        self,
        user_item_data: sp.csr_matrix,
        interactions_grouped: pl.DataFrame,
        preprocessor: DataPreprocessor,
        k: int = 10,
    ) -> ModelParams:
        study = optuna.create_study(direction="maximize")
        objective_func = partial(
            self.objective,
            user_item_data=user_item_data,
            interactions_grouped=interactions_grouped,
            preprocessor=preprocessor,
            k=k,
        )

        study.optimize(objective_func, n_trials=self.n_trials)

        best_params = study.best_params
        best_params["random_state"] = 42

        return ModelParams(**best_params)


def calculate_als_recommendations(
    interactions: pl.DataFrame, redis_manager: RedisManager
):
    preprocessor = DataPreprocessor()
    # optimizer = HyperparameterOptimizer()

    logger.info(f"unique users interactions: {interactions['user_id'].n_unique()}")
    interactions_grouped, user_item_data = preprocessor.prepare_features(interactions)

    if interactions_grouped is None and user_item_data is None:
        logger.info("No interactions provided")
        return

    logger.info(
        f"unique users interactions_grouped: {interactions_grouped['user_id'].n_unique()}",
    )
    # best_params = optimizer.optimize(user_item_data, interactions_grouped, preprocessor)

    recommender = ALSRecommender(settings.regular_pipeline_settings)
    recommender.fit(user_item_data)

    _ = recommender.recommend_all(
        user_item_data=user_item_data,
        interactions_grouped=interactions_grouped,
        preprocessor=preprocessor,
        redis_manager=redis_manager,
        k=10,
    )

    logger.info(f"len of recommendations: {len(_)} = {_}")


async def calculate_recommendations(redis_manager: RedisManager) -> None:
    i = 0
    while True:
        if os.path.exists("data/interactions.csv"):
            i += 1
            interactions = pl.read_csv("data/interactions.csv")
            logger.info(f"{i} iteration")
            logger.info("calculating top recommendations")
            calculate_top_recommendations(interactions, redis_manager)
            logger.info("Top recommendations calculated")
            logger.info(
                f"calculating als recommendations, shape: {interactions.shape}",
            )
            calculate_als_recommendations(interactions, redis_manager)
        logger.info("Als recommendations calculated")

        await asyncio.sleep(10)


async def main():
    redis_manager = RedisManager(settings.redis_settings)
    try:
        await asyncio.gather(
            collect_messages(settings),
            calculate_recommendations(redis_manager),
        )
    except Exception as e:
        logger.error("An error occurred", exc_info=e)
        raise
    finally:
        redis_manager.close()


if __name__ == "__main__":
    asyncio.run(main())
