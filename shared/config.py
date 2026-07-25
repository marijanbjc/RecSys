from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


class RedisSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="REDIS_")
    HOST: str
    PORT: int = 6379
    DB: int = 0
    INTERACTION_PREFIX: str = "interaction"
    ALS_RECOMMENDATION_PREFIX: str = "als_recommendation"
    TOP_RECOMMENDATION_PREFIX: str = "top_recommendation"


class RabbitSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="RABBIT_")
    QUEUE_NAME: str = "user_interactions"
    ROUTING_KEY: str = "user.interact.message"
    EXCHANGE: str = "user.interact"
    USER: str
    HOST: str
    PORT: int = 5672
    URL: str = "amqp://guest:guest@rabbitmq:5672/"


class WatchedFilterSettings(BaseModel):
    model_config = SettingsConfigDict(env_prefix="WF_")
    PREFIX: str = "watched"


class RegularPipelineSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="REGULAR_PIPE_")
    MESSAGES_COLLECTION_INTERVAL: int = 10
    ALS_FACTORS: int = 64
    ALS_ITERATIONS: int = 15
    ALS_ALPHA: float = 1.0
    ALS_REGULARIZATION: float = 0.1
    ALS_RANDOM_STATE: float = 42


class ServicesSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SERVICES_")
    recommendation_service_url: str = "http://46.62.174.201:5001"
    interactions_service_url: str = "http://46.62.174.201:5000"
    webapp_port: int = 8000


class RecommendationSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="RECOMMENDATION_")
    EPSILON: float = 0.05
    TOP_K: int = 10


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")
    LOG_LEVEL: str = "INFO"
    redis_settings: RedisSettings = RedisSettings()
    rabbit_settings: RabbitSettings = RabbitSettings()
    watched_filter_settings: WatchedFilterSettings = WatchedFilterSettings()
    recommendation_settings: RecommendationSettings = RecommendationSettings()


settings = AppSettings()
