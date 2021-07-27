from typing import Optional

from fastapi import APIRouter, Body
from fastapi.encoders import jsonable_encoder

from app.daos.publication import (
    retrieve_publication,
    retrieve_publications,
    top_publications,
    count_publications,
    group_count_publications,
    get_types,
    get_sources,
    get_top_lang,
    get_top_authors,
    get_top_entities,
    get_top_hashtags,
    get_tweet_time_of_day,
)
from app.models.publication import (
    ErrorResponseModel,
    ResponseModel,
    PublicationSchema,
    UpdatePublicationModel,
)

router = APIRouter()


@router.get("/", response_description="publications retrieved")
async def get_publications():
    publications = await retrieve_publications()
    if publications:
        return ResponseModel(publications, "publications data retrieved successfully")
    return ResponseModel(publications, "Empty list returned")


@router.get("/top", response_description="top retrieved")
async def get_top(limit):
    publications = await top_publications(limit)
    if publications:
        return ResponseModel(publications, "publications data retrieved successfully")
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


# @router.get("/{id}", response_description="publication data retrieved")
# async def get_publication_data(id):
#     publication = await retrieve_publication(id)
#     if publication:
#         return ResponseModel(publication, "publication data retrieved successfully")
#     return ErrorResponseModel("An error occurred.", 404, "publication doesn't exist.")

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
