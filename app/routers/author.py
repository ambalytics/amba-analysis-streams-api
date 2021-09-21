import logging
from typing import List
from urllib.parse import unquote

from app.models.schema import StatValue, Publication, TimeValue
from fastapi import APIRouter, Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session  # type: ignore

from app.daos.database import SessionLocal, engine
from app.daos.author import (
        get_authors,
        retrieve_author,
        get_trending_authors,
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


@router.get("/")
def get_authors_router(
        offset: int = 0, limit: int = 10, sort: str = 'id', order: str = 'asc', search: str = '', session: Session = Depends(get_session)
):
    items = get_authors(session=session, offset=offset, limit=limit, sort=sort, order=order, search=search)
    return items


@router.get("/trending")
def get_trending_authors_router(
        offset: int = 0, limit: int = 10, sort: str = 'score', order: str = 'desc', search: str = '', session: Session = Depends(get_session)
):
    item = get_trending_authors(session=session, offset=offset, limit=limit, sort=sort, order=order, search=search)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)

@router.get("/get")
def get_ield_of_study_data(id: int, session: Session = Depends(get_session)):
    item = retrieve_author(session, id)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)


