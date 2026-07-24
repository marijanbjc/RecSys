import random

import numpy as np
import redis
from fastapi import FastAPI

from models import RecommendationsResponse, NewItemsEvent
from watched_filter import WatchedFilter

app = FastAPI()

redis_connection = redis.Redis('redis')
watched_filter = WatchedFilter()

unique_item_ids = set()
EPSILON = 0.05

@app.get('/healthcheck')
def healthcheck():
    return 200


@app.get('/cleanup')
def cleanup():
    global unique_item_ids
    unique_item_ids = set()

    redis_connection.json().delete('*')
    redis_connection.flushall()

    return 200


@app.post('/add_items')
def add_movie(request: NewItemsEvent):
    global unique_item_ids

    for item_id in request.item_ids:
        unique_item_ids.add(item_id)

    return 200

@app.get('/recs/{user_id}')
def get_recs(user_id: str):
    global unique_item_ids
    try:
        user_history = redis_connection.keys(f"{user_id}*")
        if len(user_history) == 0:
            print(f"No history for {user_id}")
            print(f"Getting for {user_id} top-items")
            top_items = redis_connection.json().get('top_items')
            print("top items: ", top_items)
            item_ids = np.random.choice(top_items, 15, replace=False)
            print("sampled top items: ", top_items)
        else:
            print(f"History for {user_id} has {len(user_history)} items")
            item_ids = redis_connection.json().get(f"{user_id}_als_items")
            if item_ids is not None:
                item_ids = [
                    item_id for item_id in item_ids
                    if watched_filter.get(user_id, item_id) is None
                ]
                print(f"Recommendations for {user_id} als-items after filtering={len(item_ids)}")
            else:
                print(f"But for {user_id} has no als-recommendations")
                item_ids = []

    except BaseException as e:
        print(f"Exception: {e}")
        item_ids = []

    if len(item_ids) == 0 or random.random() < EPSILON and len(unique_item_ids) > 0:
        random_ids = np.random.choice(
            list(unique_item_ids), size=max(17 - len(item_ids), 2), replace=False
        ).tolist()
        print("random ids: ", random_ids)
        item_ids += random_ids

    return RecommendationsResponse(item_ids=item_ids)


# docker run -d --rm -p 6379:6379 redis/redis-stack-server:latest
# docker run -d --rm --name rabbitmq -p 5672:5672 -p 5673:5673 -p 15672:15672 rabbitmq:3-management

# sftp root@135.181.37.248
# mkdir final_project
# put -R * final_project/
# pip install --no-cache-dir -r requirements.txt
# uvicorn event_collector.main:app --host 0.0.0.0 --port 5000
# uvicorn recommendations.main:app --host 0.0.0.0 --port 5001
# uvicorn webapp.app:app --host 0.0.0.0

# docker build -t event_collector event_collector/.
# docker build -t recommendations recommendations/.
# docker run -d --rm event_collector
# docker run -d --rm recommendations