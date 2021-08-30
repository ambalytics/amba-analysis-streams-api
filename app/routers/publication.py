import logging
from typing import Optional
from urllib.parse import unquote

from fastapi import APIRouter, Body
from fastapi.encoders import jsonable_encoder

from app.daos.publication import (
    retrieve_publication,
    retrieve_publications,
    top_publications,
    twitter_data_publication,
    get_publication_count,
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


@router.get("/count", response_description="publications retrieved")
async def get_publications_count():
    publications = await get_publication_count()
    if publications:
        return ResponseModel(publications, "publications count retrieved successfully")
    return ResponseModel(publications, "Empty list returned")


# todo use doi, regex? start with 1
@router.get("/get/{s}/{p}", response_description="publication data retrieved")
async def get_publication_data(s, p):
    pd = unquote(p)
    logging.warning('retrieve publication ' + s + '/' + pd)
    publication = await retrieve_publication(s + '/' + pd)
    if publication:
        return ResponseModel(publication, "publication data retrieved successfully")
    return ErrorResponseModel("An error occurred.", 404, "publication doesn't exist.")


@router.get("/twitter/{s}/{p}", response_description="publication data retrieved")
async def get_twitter_publication_data(s, p):
    publication = await twitter_data_publication(s + '/' + p)
    if publication:
        return ResponseModel(publication, "publication data retrieved successfully")
    return ErrorResponseModel("An error occurred.", 404, "publication doesn't exist.")


@router.get("/top", response_description="top retrieved")
async def get_top(limit):
    publications = await top_publications(limit)
    if publications:
        return ResponseModel(publications, "publications data retrieved successfully")
    return ResponseModel(publications, "Empty list returned")
