import os

import redis


class WatchedFilter:
    def __init__(self):
        self.redis_connection = redis.Redis(
            os.environ.get('REDIS_HOST', 'redis'), # localhost
            port=int(os.environ.get('REDIS_PORT', 6379))
        )

    def add(self, user_id, item_ids):
        try:
            for item_id in item_ids:
                self.redis_connection.set(f'{user_id}-{item_id}', 1)
        except redis.exceptions.ConnectionError:
            # ignore errors if redis unavailable
            pass

    def get(self, user_id, item_id):
        flg = self.redis_connection.get(f'{user_id}-{item_id}')
        if flg is None:
            return None
        return flg.decode()


    def remove_all(self):
        try:
            self.redis_connection.delete('*')
        except redis.exceptions.ConnectionError:
            # ignore errors if redis unavailable
            pass
