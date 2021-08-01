from fastapi import APIRouter, Body
from fastapi.encoders import jsonable_encoder

from app.daos.stats import (
    words_from_tweet,
)
from app.models.stats import (
    ErrorResponseModel,
    ResponseModel,
    StatsSchema,
)

router = APIRouter()

@router.get("/words", response_description="count retrieved")
async def get_words():
    publications = await words_from_tweet()
    if publications:
        return ResponseModel(publications, "event data retrieved successfully")
    return ResponseModel(publications, "Empty list returned")
