import redis

from shared.config import AppSettings


class WatchedFilter:
    def __init__(self, settings: AppSettings) -> None:
        self.watched_prefix = settings.watched_filter_settings.PREFIX
        self.redis_connection = redis.Redis(
            host=settings.redis_settings.HOST,
            port=settings.redis_settings.PORT,
            db=settings.redis_settings.DB,
        )

    def add(self, user_id: str, item_ids: list[str]) -> None:
        try:
            for item_id in item_ids:
                self.redis_connection.set(
                    f"{self.watched_prefix}-{user_id}-{item_id}", 1
                )
        except redis.exceptions.ConnectionError:
            # ignore errors if redis unavailable
            pass

    def get(self, user_id: str, item_id: str) -> int | None:
        flg = self.redis_connection.get(f"{self.watched_prefix}-{user_id}-{item_id}")
        if flg is None:
            return None
        return flg.decode()

    def filter_user_items(self, user_id: str, item_ids: list[str]) -> list[str]:
        filtered_item_ids = []
        for item_id in item_ids:
            if self.get(user_id, item_id) is None:
                filtered_item_ids.append(item_id)
        return filtered_item_ids

    def remove_all(self) -> None:
        try:
            self.redis_connection.delete(f"{self.watched_prefix}-*")
        except redis.exceptions.ConnectionError:
            # ignore errors if redis unavailable
            pass
