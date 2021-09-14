from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session  # type: ignore
from app.models.schema import StatValue, Publication, TimeValue

from app.daos.database import SessionLocal, engine
from app.daos.stats import (
    get_followers_reached,
    get_top_lang,
    get_words,
    get_types,
    get_sources,
    get_top_entities,
    get_top_hashtags,
    get_tweet_count,
    get_tweet_author_count,
    get_total_score,
    get_country_list,
    get_time_count_binned,
    get_tweet_time_of_day,
    get_sentiment,
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


@router.get("/numbers", response_description="numbers retrieved")
def get_numbers(fields: Optional[List[str]] = Query(None), doi: Optional[str] = None, session: Session = Depends(get_session)):
    if not fields:
        fields = ['tweetCount']

    field_to_function = {
        'tweetCount': get_tweet_count,
        'followersReached': get_followers_reached,
        'authorCount': get_tweet_author_count,
        'totalScore': get_total_score,
    }

    json_compatible_item_data = {}

    for field in fields:
        if field not in field_to_function:
            return PlainTextResponse(content='field ' + field + ' not found')
        item = field_to_function[field](session=session, doi=doi)[0]
        json_compatible_item_data[field] = jsonable_encoder(item)

    return JSONResponse(content=json_compatible_item_data)


@router.get("/top", response_description="top words with count retrieved")
def get_top_values(fields: Optional[List[str]] = Query(None), doi: Optional[str] = None, limit: int = 10,
                   min_percentage: float = 1, session: Session = Depends(get_session)):
    if not fields:
        fields = ['languages']

    field_to_function = {
        'languages': get_top_lang,
        'words': get_words,
        'types': get_types,
        'sources': get_sources,
        'entities': get_top_entities,
        'hashtags': get_top_hashtags,
        'countries': get_country_list,
        'sentiment': get_sentiment,
    }

    json_compatible_item_data = {}

    for field in fields:
        if field not in field_to_function:
            return PlainTextResponse(content='field ' + field + ' not found')
        item = field_to_function[field](session=session, doi=doi, limit=limit, min_percentage=min_percentage)
        json_compatible_item_data[field] = jsonable_encoder(item)

    return JSONResponse(content=json_compatible_item_data)


@router.get("/words", response_model=List[StatValue])
def get_top_words(doi: Optional[str] = None, limit: int = 100, session: Session = Depends(get_session)):
    item = get_words(session, doi, limit)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)


@router.get("/types", response_model=List[StatValue])
def get_item_types(doi: Optional[str] = None, session: Session = Depends(get_session)):
    item = get_types(session, doi)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)


@router.get("/sources", response_model=List[StatValue])
def get_item_sources(doi: Optional[str] = None, session: Session = Depends(get_session)):
    item = get_sources(session, doi)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)


# tweet author languages
@router.get("/lang", response_model=List[StatValue])
def get_item_lang(doi: Optional[str] = None, session: Session = Depends(get_session)):
    item = get_top_lang(session, doi)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)


# tweet author entities
@router.get("/entities", response_model=List[StatValue])
def get_item_entities(doi: Optional[str] = None, limit: int = 10, min_percentage: int = 1,
                      session: Session = Depends(get_session)):
    item = get_top_entities(session, doi, limit, min_percentage)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)


# tweet author hashtags
@router.get("/hashtags", response_model=List[StatValue])
def get_item_hashtags(doi: Optional[str] = None, session: Session = Depends(get_session)):
    item = get_top_hashtags(session, doi)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)

# get author locations
@router.get("/locations", response_model=List[StatValue])
def get_country_locations(doi: Optional[str] = None, session: Session = Depends(get_session)):
    item = get_country_list(session, doi)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)


# get total followers reached
@router.get("/followers", response_model=StatValue)
def get_followers(doi: Optional[str] = None, session: Session = Depends(get_session)):
    item = get_followers_reached(session, doi)[0]
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)


# get author count
@router.get("/authorcount", response_model=StatValue)
def get_author_count(doi: Optional[str] = None, session: Session = Depends(get_session)):
    item = get_tweet_author_count(session, doi)[0]
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)


# get author count
@router.get("/tweetcount", response_model=StatValue)
def get_count_tweet(doi: Optional[str] = None, session: Session = Depends(get_session)):
    item = get_tweet_count(session, doi)[0]
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)


# get sum of scores
@router.get("/scoresum", response_model=StatValue)
def get_summed_score(doi: Optional[str] = None, session: Session = Depends(get_session)):
    item = get_total_score(session, doi)[0]
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)


# get count binned by time
@router.get("/timebinned", response_model=List[StatValue])
def get_time_binned(doi: Optional[str] = None, session: Session = Depends(get_session)):
    item = get_time_count_binned(session, doi)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)

# get hour binned periodic count
@router.get("/dayhour", response_model=List[StatValue])
def get_item_dayhour(doi: Optional[str] = None, session: Session = Depends(get_session)):
    item = get_tweet_time_of_day(session, doi)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)