from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session  # type: ignore

from app.daos.database import SessionLocal, engine
from app.daos.stats import (
    get_followers_reached,
    get_top_lang,
    get_words,
    get_types,
    get_sources,
    get_top_entities,
    get_top_hashtags,
    get_tweet_time_of_day,
    get_tweet_count,
    get_tweet_author_count,
    get_total_score,
    get_country_list,
    get_time_count_binned,
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


@router.get("/words", response_description="count retrieved")
def get_t_words(id, session: Session = Depends(get_session)):
    item = get_words(session, id)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)


@router.get("/types", response_description="item data retrieved")
def get_item_types(id: Optional[str] = None, session: Session = Depends(get_session)):
    item = get_types(session, id)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)


@router.get("/sources", response_description="item data retrieved")
def get_item_sources(id: Optional[str] = None, session: Session = Depends(get_session)):
    item = get_sources(session, id)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)


# tweet author languages
@router.get("/lang", response_description="item data retrieved")
def get_item_lang(id: Optional[str] = None, session: Session = Depends(get_session)):
    item = get_top_lang(session, id)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)


# tweet author entities
@router.get("/entities", response_description="item data retrieved")
def get_item_entities(id: Optional[str] = None, session: Session = Depends(get_session)):
    item = get_top_entities(session, id)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)


# tweet author hashtags
@router.get("/hashtags", response_description="item data retrieved")
def get_item_hashtags(id: Optional[str] = None, session: Session = Depends(get_session)):
    item = get_top_hashtags(session, id)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)


# get hour binned periodic count
@router.get("/dayhour", response_description="item data retrieved")
def get_item_dayhour(id: Optional[str] = None, session: Session = Depends(get_session)):
    item = get_tweet_time_of_day(session, id)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)


# get author locations
@router.get("/locations", response_description="item data retrieved")
def get_country_locations(id: Optional[str] = None, session: Session = Depends(get_session)):
    item = get_country_list(session, id)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)


# get total followers reached
@router.get("/followers", response_description="item data retrieved")
def get_followers(id: Optional[str] = None, session: Session = Depends(get_session)):
    item = get_followers_reached(session, id)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)


# get author count
@router.get("/authorcount", response_description="item data retrieved")
def get_author_count(id: Optional[str] = None, session: Session = Depends(get_session)):
    item = get_tweet_author_count(session, id)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)


# get author count
@router.get("/tweetcount", response_description="item data retrieved")
def get_count_tweet(id: Optional[str] = None, session: Session = Depends(get_session)):
    item = get_tweet_count(session, id)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)


# get sum of scores
@router.get("/scoresum", response_description="item data retrieved")
def get_summed_score(id: Optional[str] = None, session: Session = Depends(get_session)):
    item = get_total_score(session, id)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)


# get count binned by time
@router.get("/timebinned", response_description="item data retrieved")
def get_time_binned(id: Optional[str] = None, session: Session = Depends(get_session)):
    item = get_time_count_binned(session, id)
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)
