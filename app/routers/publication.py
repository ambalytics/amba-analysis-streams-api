import logging
from typing import List
from urllib.parse import unquote

from app.models.schema import StatValue, Publication, TimeValue
from fastapi import APIRouter, Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session  # type: ignore

from app.daos.database import SessionLocal, engine
from app.daos.publication import (
    get_publication_count,
    retrieve_publication,
    get_publications_db,
    top_publications,
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
def get_publications(
        offset: int = 0, limit: int = 10, sort: str = 'id', order: str = 'asc', session: Session = Depends(get_session)
):
    publications = get_publications_db(session=session, offset=offset, limit=limit, sort=sort, order=order)
    return publications


@router.get("/count", response_model=StatValue)
def get_publications_count(session: Session = Depends(get_session)):
    item = get_publication_count(session=session)[0]
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)


# # todo use doi, regex? start with 1 ,  response_model=Publication
@router.get("/get/{s}/{p}")
def get_publication_data(s, p, session: Session = Depends(get_session)):
    pd = unquote(p)
    logging.warning('retrieve publication ' + s + '/' + pd)
    publication = retrieve_publication(session, s + '/' + pd)
    logging.warning(publication)
    json_compatible_item_data = jsonable_encoder(publication)
    return JSONResponse(content=json_compatible_item_data)


@router.get("/top")
def get_top(limit: int = 20, session: Session = Depends(get_session)):
    publications = top_publications(session, limit)
    json_compatible_item_data = jsonable_encoder(publications)
    return JSONResponse(content=json_compatible_item_data)
