import logging
import time
from urllib.parse import unquote

from app.models.schema import StatValue, Publication, TimeValue, AmbaResponse
from fastapi import APIRouter, Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session  # type: ignore

from app.daos.database import SessionLocal, engine, query_api
from app.daos.publication import (
    retrieve_publication,
    get_publications,
    get_trending_publications,
    get_trending_publications_for_field_of_study,
    get_trending_covid_publications,
    get_trending_publications_for_author
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


@router.get("/trending", summary="Get trending publications.", response_model=AmbaResponse)
def get_trending_publications_router(
        offset: int = 0, limit: int = 10, sort: str = 'score', order: str = 'desc', search: str = '',
        duration: str = "currently", session: Session = Depends(get_session)
):
    """
    Return publication trending data for a given duration.

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
    item = get_trending_publications(session=session, offset=offset, limit=limit, sort=sort, order=order,
                                     duration=duration, search=search)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content={"time": round((time.time() - start) * 1000), "results": json_compatible_item_data})


@router.get("/trending/covid", summary="Get trending covid publications.", response_model=AmbaResponse)
def get_trending__covid_publications_router(
        offset: int = 0, limit: int = 10, sort: str = 'score', order: str = 'desc', search: str = '',
        duration: str = "currently", session: Session = Depends(get_session)
):
    """
    Return covid related publication trending data for a given duration.

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
    item = get_trending_covid_publications(session=session, offset=offset, limit=limit, sort=sort, order=order,
                                           duration=duration, search=search)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content={"time": round((time.time() - start) * 1000), "results": json_compatible_item_data})


@router.get("/trending/fieldOfScience", summary="Get trending publications for a given field of study.",
            response_model=AmbaResponse)
def get_trending_publications_for_field_of_study_router(id: int,
                                                        offset: int = 0, limit: int = 10, sort: str = 'score',
                                                        order: str = 'desc', search: str = '',
                                                        duration: str = "currently",
                                                        session: Session = Depends(get_session)
                                                        ):
    """
    Return publications and their trending data for a given duration and field of study.

    - **id**: fieldOfStudyId
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
    item = get_trending_publications_for_field_of_study(session=session, offset=offset, limit=limit, sort=sort,
                                                        order=order, duration=duration, search=search, fos_id=id)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content={"time": round((time.time() - start) * 1000), "results": json_compatible_item_data})


@router.get("/trending/author", summary="Get trending publications for a given author.", response_model=AmbaResponse)
def get_trending_publications_for_author_router(id: int,
                                                offset: int = 0, limit: int = 10, sort: str = 'score',
                                                order: str = 'desc', search: str = '',
                                                duration: str = "currently", session: Session = Depends(get_session)
                                                ):
    """
    Return publications and their trending data for a given duration and author.

    - **id**: authorId
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
    item = get_trending_publications_for_author(session=session, offset=offset, limit=limit, sort=sort, order=order,
                                                duration=duration, search=search, author_id=id)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content={"time": round((time.time() - start) * 1000), "results": json_compatible_item_data})


# # todo use doi, regex? start with 1 ,  response_model=Publication
@router.get("/get", summary="Get publication.", response_model=AmbaResponse)
def get_publication_data(doi: str, duration: str = "currently", session: Session = Depends(get_session)):
    """
    get publication data for a given doi

    - **doi**: doi of the publication to get
    - **duration**: the duration of data that should be queried, 'currently' (default), 'today', 'week', 'month',
        'year'
    """
    logging.warning('retrieve publication ' + doi)
    logging.warning('retrieve publication ' + unquote(doi))

    start = time.time()
    publication = retrieve_publication(session, doi, duration)
    logging.warning(publication)
    json_compatible_item_data = jsonable_encoder(publication)
    return JSONResponse(content={"time": round((time.time() - start) * 1000), "results": json_compatible_item_data})
