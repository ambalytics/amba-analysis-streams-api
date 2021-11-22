import time
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session  # type: ignore
from app.models.schema import StatValue, Publication, TimeValue, DiscussionNewestSubj, AmbaResponse

from app.daos.database import SessionLocal, engine, query_api
from app.daos.field_of_study import (
    retrieve_field_of_study
)
from app.daos.author import (
    retrieve_author
)
from app.daos.stats import (
    get_discussion_data_list,
    get_discussion_data_list_with_percentage,
    get_trending_chart_data,
    get_window_chart_data,
    get_numbers_influx,
    get_profile_information_avg,
    get_profile_information_for_doi,
    get_dois_for_author,
    get_dois_for_field_of_study,
    get_tweets,
    get_total_tweet_count,
    get_tweet_author_count
)
import event_stream.models.model as models
from starlette.responses import JSONResponse, PlainTextResponse

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


@router.get("/numbers", summary="Get statistical numbers.", response_model=AmbaResponse)
def get_numbers(fields: Optional[List[str]] = Query(None), dois: Optional[List[str]] = Query(None),
                duration: str = "currently", mode: str = "publication", id: int = None,
                session: Session = Depends(get_session)):
    """
    Query statistical numbers for publications.

    - **fields**: list of strings with one of the following values, 'bot_rating', 'contains_abstract_raw',
        'exclamations', 'followers', 'length', 'questions', 'score', 'sentiment_raw', "count" (default)
    - **dois**: (optional) only use the given dois
    - **duration**: the duration of data that should be queried, 'currently' (default), 'today', 'week', 'month', 'year'
    - **mode**: what mode should be used, can be: 'publication' (default), 'fieldOfStudy' or 'author'
    - **id**: needed for 'fieldOfStudy' or 'author' mode, the id of the entity
    """
    start = time.time()

    if mode == "fieldOfStudy" and id:
        dois = get_dois_for_field_of_study(id, session, duration)

    if mode == "author" and id:
        dois = get_dois_for_author(id, session, duration)

    if not fields:
        fields = ['count']

    json_compatible_item_data = get_numbers_influx(query_api=query_api, dois=dois, duration=duration, fields=fields)

    return JSONResponse(content={"time": round((time.time() - start) * 1000), "results": json_compatible_item_data})


@router.get("/top", summary="Get top numbers.", response_model=AmbaResponse)
def get_top_values(fields: Optional[List[str]] = Query(None), doi: Optional[str] = None, limit: int = 10,
                   mode: str = "publication", id: int = None, session: Session = Depends(get_session)):
    """
        Query accumulated top data numbers for publications. This query does not have a duration and will always return
        data collected over all time.

        - **fields**: list of strings with one of the following values, 'entity', 'hashtag', 'lang', 'location', 'name',
            'source', 'tweet_type', 'word' (default)
        - **doi**: (optional) only use the given doi
        - **limit**: (optional, 10) limits the result
        - **mode**: what mode should be used, can be: 'publication' (default), 'fieldOfStudy' or 'author'
        - **id**: needed for 'fieldOfStudy' or 'author' mode, the id of the entity
        """
    start = time.time()

    if not fields:
        fields = ['word']

    json_compatible_item_data = {}

    for field in fields:
        item = get_discussion_data_list(session=session, doi=doi, limit=limit, dd_type=field, id=id, mode=mode)
        json_compatible_item_data[field] = jsonable_encoder(item)

    return JSONResponse(content={"time": round((time.time() - start) * 1000), "results": json_compatible_item_data})


@router.get("/top/percentages", summary="Get top numbers with percentage.", response_model=AmbaResponse)
def get_top_percentage_values(fields: Optional[List[str]] = Query(None), doi: Optional[str] = None, limit: int = 10,
                              min_percentage: float = 1, session: Session = Depends(get_session)):
    """
        Query accumulated top data numbers for publications with a percentage as well as a min percentage to filter out
        rare items.  This query does not have a duration and will always return data collected over all time.

        - **fields**: list of strings with one of the following values, 'entity', 'hashtag', 'lang', 'location', 'name',
            'source', 'tweet_type', 'word' (default)
        - **doi**: (optional) only use the given doi
        - **limit**: (optional, 10) limits the result
        - **min_percentage**: (optional, 1) limits the results to only items that have a higher or equal percentage
        """
    start = time.time()

    if not fields:
        fields = ['lang']

    json_compatible_item_data = {}

    for field in fields:
        item = get_discussion_data_list_with_percentage(session=session, doi=doi, limit=limit,
                                                        min_percentage=min_percentage, dd_type=field)
        json_compatible_item_data[field] = jsonable_encoder(item)

    return JSONResponse(content={"time": round((time.time() - start) * 1000), "results": json_compatible_item_data})


# get profile information for a publication by doi
@router.get("/profile", summary="Get top profile information.", response_model=AmbaResponse)
def get_profile_information(doi: Optional[str] = Query(None), duration: Optional[str] = "currently",
                            mode: str = "publication", id: int = None, session: Session = Depends(get_session)):
    """
        Return profile information meaning it will not only return the value of the doi, author or field of study but
        the avg, min and max to compare against.

        - **dois**: (optional) only use the given dois
        - **duration**: the duration of data that should be queried, 'currently' (default), 'today', 'week', 'month',
            'year'
        - **mode**: what mode should be used, can be: 'publication' (default), 'fieldOfStudy' or 'author'
        - **id**: needed for 'fieldOfStudy' or 'author' mode, the id of the entity
    """
    start = time.time()

    if (mode == "publication" and not doi) or (mode == "fieldOfStudy" and not id) or (mode == "author" and not id):
        raise HTTPException(status_code=404, detail="Missing data.")

    doi_info = {
        'publication': get_profile_information_for_doi(session, doi, id, mode, duration)
    }

    if mode == "publication" and doi and doi_info:
        doi_info['publication']['doi'] = doi
    if mode == "fieldOfStudy" and id and doi_info:
        doi_info['publication']['doi'] = retrieve_field_of_study(session, id)['fields_of_study']
    if mode == "author" and id and doi_info:
        doi_info['publication']['doi'] = retrieve_author(session, id)['author']
    avg_info = get_profile_information_avg(session, duration)

    if doi_info and avg_info:
        json_compatible_item_data = jsonable_encoder({**doi_info, **avg_info})
    else:
        json_compatible_item_data = {}
    return JSONResponse(content={"time": round((time.time() - start) * 1000), "results": json_compatible_item_data})


# get chart data
@router.get("/progress/value", summary="Get progress for publications.", response_model=AmbaResponse)
def get_window_progress(field: Optional[str] = Query(None), n: Optional[int] = 5, duration: Optional[str] = "currently",
                        dois: Optional[List[str]] = Query(None), mode: str = "publication", id: int = None,
                        session: Session = Depends(get_session)):
    """
        Return the progress over time for a given field. It will either use the top n publications or a given doi list.
        Data will be aggregated in windows to optimize performance.

        - **field**: list of strings with one of the following values: 'bot_rating', 'contains_abstract_raw',
            'exclamations', 'followers', 'length', 'questions', 'score' (default), 'sentiment_raw', 'count'
        - **n**: if no dois given use the top n dois (based on the current duration)
        - **duration**: the duration of data that should be queried, 'currently' (default), 'today', 'week', 'month',
            'year'
        - **dois**: (optional) only use the given dois
        - **mode**: what mode should be used, can be: 'publication' (default), 'fieldOfStudy' or 'author'
        - **id**: needed for 'fieldOfStudy' or 'author' mode, the id of the entity
    """
    start = time.time()

    if not field:
        field = 'score'

    if mode == "fieldOfStudy" and id:
        dois = get_dois_for_field_of_study(id, session, duration)

    if mode == "author" and id:
        dois = get_dois_for_author(id, session, duration)

    json_compatible_item_data = get_window_chart_data(query_api, session, duration, field, n, dois)
    return JSONResponse(content={"time": round((time.time() - start) * 1000), "results": json_compatible_item_data})


# get trending chart data
@router.get("/progress/trending", summary="Get progress from the trending bucket.", response_model=AmbaResponse)
def get_trending_progress(field: Optional[str] = Query(None), n: Optional[int] = 5,
                          duration: Optional[str] = "currently", dois: Optional[List[str]] = Query(None),
                          mode: str = "publication", id: int = None, session: Session = Depends(get_session)):
    """
        Return the trending progress over time for a given field. It will either use the top n publications or a given
        doi list.

        - **field**: list of strings with one of the following values: 'score' (default), 'count', 'mean_sentiment',
            'sum_followers', 'abstract_difference', 'mean_age', 'mean_length', 'mean_questions', 'mean_exclamations',
            'mean_bot_rating', 'projected_change', 'trending', 'ema', 'kama', 'ker', 'mean_score', 'stddev'
        - **n**: if no dois given use the top n dois (based on the current duration)
        - **duration**: the duration of data that should be queried, 'currently' (default), 'today', 'week', 'month',
            'year'
        - **dois**: (optional) only use the given dois
        - **mode**: what mode should be used, can be: 'publication' (default), 'fieldOfStudy' or 'author'
        - **id**: needed for 'fieldOfStudy' or 'author' mode, the id of the entity
    """
    start = time.time()

    if not field:
        field = 'score'

    if mode == "fieldOfStudy" and id:
        dois = get_dois_for_field_of_study(id, session, duration)

    if mode == "author" and id:
        dois = get_dois_for_author(id, session, duration)

    json_compatible_item_data = get_trending_chart_data(query_api, session, duration, field, n, dois)
    return JSONResponse(content={"time": round((time.time() - start) * 1000), "results": json_compatible_item_data})


# get newest tweets
@router.get("/tweets", summary="Get newest discussion data.", response_model=AmbaResponse)
def get_tweets_discussion_data(doi: Optional[str] = Query(None), mode: str = "publication", id: int = None,
                               session: Session = Depends(get_session)):
    """
        Get the newest discussion data.

        - **doi**: (optional) only use the given doi
        - **mode**: what mode should be used, can be: 'publication' (default), 'fieldOfStudy' or 'author'
        - **id**: needed for 'fieldOfStudy' or 'author' mode, the id of the entity
    """
    start = time.time()
    json_compatible_item_data = [get_tweets(doi=doi, session=session, id=id, mode=mode)]
    return {"time": round((time.time() - start) * 1000), "results": json_compatible_item_data}


# get tweet author count
@router.get("/countTweets", summary="Get total tweet count.", response_model=AmbaResponse)
def get_count_total_tweets(doi: Optional[str] = Query(None), mode: str = "publication", id: int = None,
                           session: Session = Depends(get_session)):
    """
        Get total tweet count.

        - **doi**: (optional) only use the given doi
        - **mode**: what mode should be used, can be: 'publication' (default), 'fieldOfStudy' or 'author'
        - **id**: needed for 'fieldOfStudy' or 'author' mode, the id of the entity
    """
    start = time.time()
    json_compatible_item_data = get_total_tweet_count(doi=doi, session=session, id=id, mode=mode)
    return {"time": round((time.time() - start) * 1000), "results": json_compatible_item_data}


@router.get("/countTweetAuthors", summary="Get total tweet author count.", response_model=AmbaResponse)
def get_count_tweet_author(doi: Optional[str] = Query(None), mode: str = "publication", id: int = None,
                           session: Session = Depends(get_session)):
    """
        Get total tweet author count.

        - **doi**: (optional) only use the given doi
        - **mode**: what mode should be used, can be: 'publication' (default), 'fieldOfStudy' or 'author'
        - **id**: needed for 'fieldOfStudy' or 'author' mode, the id of the entity
    """
    start = time.time()
    json_compatible_item_data = get_tweet_author_count(doi=doi, session=session, id=id, mode=mode)
    return {"time": round((time.time() - start) * 1000), "results": json_compatible_item_data}
