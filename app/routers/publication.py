from typing import Optional

from fastapi import APIRouter, Body
from fastapi.encoders import jsonable_encoder

from app.daos.publication import (
    retrieve_publication,
    retrieve_publications,
    top_publications,
    twitter_data_publication,
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

# todo use doi, regex? start with 1
@router.get("/get/{s}/{p}", response_description="publication data retrieved")
async def get_publication_data(s, p):
    publication = await retrieve_publication(s + '/' + p)
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

