from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session  # type: ignore
from app.models.schema import StatValue, Publication, TimeValue, DiscussionNewestSubj

from app.daos.database import SessionLocal, engine, query_api
from app.daos.stats import (
    get_discussion_data_list,
    get_discussion_data_list_with_percentage,
    get_trending_chart_data,
    get_window_chart_data,
    get_number_influx,
    get_profile_information_avg,
    get_profile_information_for_doi,
    get_dois_for_author,
    get_dois_for_field_of_study,
    get_tweets
)
import event_stream.models.model as models
from starlette.responses import JSONResponse, PlainTextResponse

models.Base.metadata.create_all(bind=engine)
router = APIRouter()


def get_session():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


# 'bot_rating', 'contains_abstract_raw', 'exclamations', 'followers', 'length', 'questions', 'score', 'sentiment_raw', "count"
@router.get("/numbers", response_description="numbers retrieved")
def get_numbers(fields: Optional[List[str]] = Query(None), dois: Optional[List[str]] = Query(None),
                duration: str = "currently", mode: str = "publication", id: int = None, session: Session = Depends(get_session)):

    if mode == "fieldOfStudy" and id:
        dois = get_dois_for_field_of_study(id, session, duration)

    if mode == "author" and id:
        dois = get_dois_for_author(id, session, duration)

    if not fields:
        fields = ['count']

    json_compatible_item_data = {}

    for field in fields:
        item = get_number_influx(query_api=query_api, dois=dois, duration=duration, field=field)
        json_compatible_item_data[field] = jsonable_encoder(item)

    return JSONResponse(content=json_compatible_item_data)


@router.get("/top", response_description="top values with count retrieved")
def get_top_values(fields: Optional[List[str]] = Query(None), doi: Optional[str] = None, limit: int = 10,
                   session: Session = Depends(get_session)):
    if not fields:
        fields = ['word']

    json_compatible_item_data = {}

    for field in fields:
        item = get_discussion_data_list(session=session, doi=doi, limit=limit, dd_type=field)
        json_compatible_item_data[field] = jsonable_encoder(item)

    return JSONResponse(content=json_compatible_item_data)


@router.get("/top/percentages", response_description="top values with percentage retrieved")
def get_top_values(fields: Optional[List[str]] = Query(None), doi: Optional[str] = None, limit: int = 10,
                   min_percentage: float = 1, session: Session = Depends(get_session)):
    if not fields:
        fields = ['lang']

    json_compatible_item_data = {}

    for field in fields:
        item = get_discussion_data_list_with_percentage(session=session, doi=doi, limit=limit,
                                                        min_percentage=min_percentage, dd_type=field)
        json_compatible_item_data[field] = jsonable_encoder(item)

    return JSONResponse(content=json_compatible_item_data)


# get profile information for a publication by doi
@router.get("/profile", response_model=List[StatValue])
def get_profile_information(dois: List[str] = Query(None), duration: Optional[str] = "currently",
                            mode: str = "publication", id: int = None, session: Session = Depends(get_session)):

    if mode == "fieldOfStudy" and id:
        dois = get_dois_for_field_of_study(id, session, duration)

    if mode == "author" and id:
        dois = get_dois_for_author(id, session, duration)

    doi_info = get_profile_information_for_doi(query_api, dois, duration)
    avg_info = get_profile_information_avg(session, duration)

    json_compatible_item_data = jsonable_encoder({**doi_info, **avg_info})
    return JSONResponse(content=json_compatible_item_data)


# get chart data
@router.get("/progress/value", response_model=List[StatValue])
def get_window_progress(field: Optional[str] = Query(None), n: Optional[int] = 5, duration: Optional[str] = "currently"):
    json_compatible_item_data = get_window_chart_data(query_api, duration, field, n)
    return JSONResponse(content=json_compatible_item_data)


# get trending chart data
@router.get("/progress/trending", response_model=List[StatValue])
def get_trending_progress(field: Optional[str] = Query(None), duration: Optional[str] = "currently"):
    json_compatible_item_data = get_trending_chart_data(query_api, duration, field)
    return JSONResponse(content=json_compatible_item_data)


# get newest tweets
@router.get("/tweets")
def get__tweets_discussion_data(doi: Optional[str] = Query(None), limit: int = 10, session: Session = Depends(get_session)):
    json_compatible_item_data = get_tweets(doi, session, limit)
    print(json_compatible_item_data)
    return json_compatible_item_data
