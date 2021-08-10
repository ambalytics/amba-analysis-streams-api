import logging
from typing import Optional

from fastapi import APIRouter, Body
from fastapi.encoders import jsonable_encoder

from app.daos.stats import (
    get_words,
    count_publications,
    group_count_publications,
    get_types,
    get_sources,
    get_top_lang,
    get_top_authors,
    get_top_entities,
    get_top_hashtags,
    get_tweet_time_of_day,
    get_country_list,
    get_tweet_author_count,
    get_followers_reached,
    get_tweet_count,
)
from app.models.stats import (
    ErrorResponseModel,
    ResponseModel,
    StatsSchema,
)

router = APIRouter()


@router.get("/words", response_description="count retrieved")
async def get_t_words(id):
    publications = await get_words(id)
    if publications:
        return ResponseModel(publications, "event data retrieved successfully")
    return ResponseModel(publications, "Empty list returned")


@router.get("/count", response_description="count retrieved")
async def get_count():
    count = await count_publications()
    if count:
        return ResponseModel(count, "publications data retrieved successfully")
    return ResponseModel(count, "Empty list returned")


@router.get("/group/count", response_description="count retrieved")
async def get_group_count(field, limit):
    publications = await group_count_publications(field, limit)
    if publications:
        return ResponseModel(publications, "publications data retrieved successfully")
    return ResponseModel(publications, "Empty list returned")


# todo move this to own statistics

@router.get("/types", response_description="publication data retrieved")
async def get_publication_types(id: Optional[str] = None):
    publication = await get_types(id)
    if publication:
        return ResponseModel(publication, "publication data retrieved successfully")
    return ErrorResponseModel("An error occurred.", 404, "publication doesn't exist.")


@router.get("/sources", response_description="publication data retrieved")
async def get_publication_sources(id: Optional[str] = None):
    publication = await get_sources(id)
    if publication:
        return ResponseModel(publication, "publication data retrieved successfully")
    return ErrorResponseModel("An error occurred.", 404, "publication doesn't exist.")


# tweet author data!
@router.get("/authors", response_description="publication data retrieved")
async def get_publication_authors(id: Optional[str] = None, original: Optional[bool] = False):
    publication = await get_top_authors(id, original)
    if publication:
        return ResponseModel(publication, "publication data retrieved successfully")
    return ErrorResponseModel("An error occurred.", 404, "publication doesn't exist.")


# tweet author languages
@router.get("/lang", response_description="publication data retrieved")
async def get_publication_lang(id: Optional[str] = None):
    publication = await get_top_lang(id)
    if publication:
        return ResponseModel(publication, "publication data retrieved successfully")
    return ErrorResponseModel("An error occurred.", 404, "publication doesn't exist.")


# tweet author entities
@router.get("/entities", response_description="publication data retrieved")
async def get_publication_entities(id: Optional[str] = None):
    publication = await get_top_entities(id)
    if publication:
        return ResponseModel(publication, "publication data retrieved successfully")
    return ErrorResponseModel("An error occurred.", 404, "publication doesn't exist.")


# tweet author hashtags
@router.get("/hashtags", response_description="publication data retrieved")
async def get_publication_hashtags(id: Optional[str] = None):
    publication = await get_top_hashtags(id)
    if publication:
        return ResponseModel(publication, "publication data retrieved successfully")
    return ErrorResponseModel("An error occurred.", 404, "publication doesn't exist.")


# get hour binned periodic count
@router.get("/dayhour", response_description="publication data retrieved")
async def get_publication_dayhour(id: Optional[str] = None):
    publication = await get_tweet_time_of_day(id)
    if publication:
        return ResponseModel(publication, "publication data retrieved successfully")
    return ErrorResponseModel("An error occurred.", 404, "publication doesn't exist.")


# get author locations
@router.get("/locations", response_description="publication data retrieved")
async def get_country_locations(id: Optional[str] = None):
    publication = await get_country_list(id)
    if publication:
        return ResponseModel(publication, "publication data retrieved successfully")
    return ErrorResponseModel("An error occurred.", 404, "publication doesn't exist.")


# get total followers reached
@router.get("/followers", response_description="publication data retrieved")
async def get_followers(id: Optional[str] = None):
    publication = await get_followers_reached(id)
    if publication:
        return ResponseModel(publication, "publication data retrieved successfully")
    return ErrorResponseModel("An error occurred.", 404, "publication doesn't exist.")


# get author count
@router.get("/authorcount", response_description="publication data retrieved")
async def get_author_count(id: Optional[str] = None):
    publication = await get_tweet_author_count(id)
    if publication:
        return ResponseModel(publication, "publication data retrieved successfully")
    return ErrorResponseModel("An error occurred.", 404, "publication doesn't exist.")

# get author count
@router.get("/tweetcount", response_description="publication data retrieved")
async def get_count_tweet(id: Optional[str] = None):
    publication = await get_tweet_count(id)
    if publication:
        return ResponseModel(publication, "publication data retrieved successfully")
    return ErrorResponseModel("An error occurred.", 404, "publication doesn't exist.")
