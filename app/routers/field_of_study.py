import logging
import time
from typing import List
from urllib.parse import unquote

from app.models.schema import StatValue, Publication, TimeValue, AmbaResponse
from fastapi import APIRouter, Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session  # type: ignore

from app.daos.database import SessionLocal, engine
from app.daos.field_of_study import (
    get_fields_of_study,
    retrieve_field_of_study,
    get_trending_fields_of_study,
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


@router.get("/", summary="Get Fields of Study.", response_model=AmbaResponse)
def get_fields_of_study_router(
        offset: int = 0, limit: int = 10, sort: str = 'id', order: str = 'asc', search: str = '',
        session: Session = Depends(get_session)):
    """
    Return fields of study data with added reduced publication data as json. This will **not** contain any trending or
    processed data.

    - **offset**: offset
    - **limit**: limit the result
    - **sort**: field to use for sort
    - **order**: 'asc' or 'desc'
    - **search**: search keyword (title only)
    """
    start = time.time()
    json_compatible_item_data = get_fields_of_study(session=session, offset=offset, limit=limit, sort=sort, order=order,
                                                    search=search)
    return {"time": round((time.time() - start) * 1000), "results": json_compatible_item_data}


@router.get("/trending", summary="Get trending Fields of Study.", response_model=AmbaResponse)
def get_trending_fields_of_study_router(
        offset: int = 0, limit: int = 10, sort: str = 'score', order: str = 'desc', search: str = '',
        duration: str = "currently", session: Session = Depends(get_session)):
    """
        Return trending fields of study and their trending data for a given duration.

        - **offset**: offset
        - **limit**: limit the result
        - **sort**: field to use for sort, available: 'score', 'count', 'mean_sentiment', 'sum_followers',
                    'abstract_difference', 'tweet_author_diversity', 'lan_diversity', 'location_diversity', 'mean_age',
                    'mean_length', 'avg_questions', 'avg_exclamations', 'projected_change'
        - **order**: 'asc' or 'desc'
        - **search**: search keyword (title only)
        - **duration**: the duration of data that should be queried, 'currently' (default), 'today', 'week', 'month',
            'year'
        """
    start = time.time()
    item = get_trending_fields_of_study(session=session, offset=offset, limit=limit, sort=sort, order=order,
                                        duration=duration, search=search)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content={"time": round((time.time() - start) * 1000), "results": json_compatible_item_data})


@router.get("/get", summary="Get Field of Study.", response_model=AmbaResponse)
def get_field_of_study_data(id: int, session: Session = Depends(get_session), with_pubs: bool = False):
    """
        Get field of study data for a given id. it will also return the publication data of all publications in a given
        field of study.

        - **id**: id of the field of study to get
        """
    start = time.time()
    item = retrieve_field_of_study(session, id, with_pubs)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content={"time": round((time.time() - start) * 1000), "results": json_compatible_item_data})
