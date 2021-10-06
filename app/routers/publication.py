import logging
from typing import List
from urllib.parse import unquote

from app.models.schema import StatValue, Publication, TimeValue
from fastapi import APIRouter, Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session  # type: ignore

from app.daos.database import SessionLocal, engine, query_api
from app.daos.database import SessionLocal, engine
from app.daos.publication import (
    retrieve_publication,
    get_publications,
    get_trending_publications,
    get_count,
    get_trending_publications_for_field_of_study,
    get_trending_publications_for_author
)
import event_stream.models.model as models
from starlette.responses import JSONResponse

models.Base.metadata.create_all(bind=engine)
router = APIRouter()


def get_session():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


@router.get("/", response_model=List[Publication])
def get_publications_router(
        offset: int = 0, limit: int = 10, sort: str = 'id', order: str = 'asc', search: str = '',
        session: Session = Depends(get_session)
):
    publications = get_publications(session=session, offset=offset, limit=limit, sort=sort, order=order, search=search)
    return publications


@router.get("/trending")
def get_trending_publications_router(
        offset: int = 0, limit: int = 10, sort: str = 'score', order: str = 'desc', search: str = '',
        duration: str = "currently", session: Session = Depends(get_session)
):
    item = get_trending_publications(session=session, offset=offset, limit=limit, sort=sort, order=order,
                                     duration=duration, search=search)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)


@router.get("/trending/fieldOfScience")
def get_trending_publications_for_field_of_study_router(id: int,
                                                        offset: int = 0, limit: int = 10, sort: str = 'score',
                                                        order: str = 'desc', search: str = '',
                                                        duration: str = "currently",
                                                        session: Session = Depends(get_session)
                                                        ):
    item = get_trending_publications_for_field_of_study(session=session, offset=offset, limit=limit, sort=sort,
                                                        order=order, duration=duration, search=search, fos_id=id)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)


@router.get("/trending/author")
def get_trending_publications_for_author_router(id: int,
                                                offset: int = 0, limit: int = 10, sort: str = 'score',
                                                order: str = 'desc', search: str = '',
                                                duration: str = "currently", session: Session = Depends(get_session)
                                                ):
    item = get_trending_publications_for_author(session=session, offset=offset, limit=limit, sort=sort, order=order,
                                                duration=duration, search=search, author_id=id)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)


@router.get("/count", response_model=StatValue)
def get_publications_count(session: Session = Depends(get_session)):
    item = get_count(query_api)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)


# # todo use doi, regex? start with 1 ,  response_model=Publication
@router.get("/get")
def get_publication_data(doi: str, session: Session = Depends(get_session)):
    logging.warning('retrieve publication ' + doi)
    logging.warning('retrieve publication ' + unquote(doi))
    publication = retrieve_publication(session, doi)
    logging.warning(publication)
    json_compatible_item_data = jsonable_encoder(publication)
    return JSONResponse(content=json_compatible_item_data)
