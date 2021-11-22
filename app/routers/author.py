import time
from app.models.schema import StatValue, Publication, TimeValue, AmbaResponse
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
    """
    get/create a session
    """
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


@router.get("/trending", summary="Get trending Authors.", response_model=AmbaResponse)
def get_trending_authors_router(
        offset: int = 0, limit: int = 10, sort: str = 'score', order: str = 'desc', search: str = '',
        duration: str = "currently", session: Session = Depends(get_session)):
    """
        Return trending authors and their trending data for a given duration.

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
    item = get_trending_authors(session=session, offset=offset, limit=limit, sort=sort, order=order,
                                duration=duration, search=search)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content={"time": round((time.time() - start) * 1000), "results": json_compatible_item_data})


@router.get("/get", summary="Get Author.", response_model=AmbaResponse)
def get_author(id: int, session: Session = Depends(get_session)):
    """
        Get author data for a given id. it will also return the publication data of all publications in a given
        author.

        - **id**: id of the author to get
    """
    start = time.time()
    item = retrieve_author(session, id)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content={"time": round((time.time() - start) * 1000), "results": json_compatible_item_data})
