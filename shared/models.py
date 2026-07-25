import time
from typing import Literal

from pydantic import BaseModel, Field


class RecommendationsResponse(BaseModel):
    item_ids: list[str] = Field([], description="list of recommended items")


class InteractEvent(BaseModel):
    user_id: str = Field(description="identifier of user")
    item_ids: list[str] = Field(description="identifiers of interacted items")
    actions: list[Literal["like", "dislike"]] = Field(
        description="positive or negative reaction for items"
    )
    timestamp: float | None = Field(time.time(), description="timestamp of event")


class NewItemsEvent(BaseModel):
    item_ids: list[str] = Field(description="identifiers of new items")
    genres: list[list[str]] = Field(description="list of genres")
