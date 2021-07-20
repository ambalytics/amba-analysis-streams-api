from fastapi import APIRouter, Body
from fastapi.encoders import jsonable_encoder

from app.daos.publication import (
    retrieve_publication,
    retrieve_publications,
    top_publications,
    count_publications,
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
async def get_top():
    publications = await top_publications()
    if publications:
        return ResponseModel(publications, "publications data retrieved successfully")
    return ResponseModel(publications, "Empty list returned")


@router.get("/count", response_description="top retrieved")
async def get_count(field, limit):
    publications = await count_publications(field, limit)
    if publications:
        return ResponseModel(publications, "publications data retrieved successfully")
    return ResponseModel(publications, "Empty list returned")


@router.get("/{id}", response_description="publication data retrieved")
async def get_publication_data(id):
    publication = await retrieve_publication(id)
    if publication:
        return ResponseModel(publication, "publication data retrieved successfully")
    return ErrorResponseModel("An error occurred.", 404, "publication doesn't exist.")
