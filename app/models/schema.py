from enum import Enum
import datetime
from typing import Optional, List
from pydantic import BaseModel, Json, PyObject


class PublicationType(str, Enum):
    BOOK = 'BOOK'
    BOOK_CHAPTER = 'BOOK_CHAPTER'
    BOOK_REFERENCE_ENTRY = 'BOOK_REFERENCE_ENTRY'
    CONFERENCE_PAPER = 'CONFERENCE_PAPER'
    DATASET = 'DATASET'
    JOURNAL_ARTICLE = 'JOURNAL_ARTICLE'
    PATENT = 'PATENT'
    REPOSITORY = 'REPOSITORY'
    THESIS = 'THESIS'
    UNKNOWN = 'UNKNOWN'


class Publication(BaseModel):
    id: int
    doi: str
    type: Optional[PublicationType]
    pub_date: Optional[str]
    year: Optional[int]
    publisher: Optional[str]
    citation_count: Optional[int]
    title: Optional[str]
    normalized_title: Optional[str]
    abstract: Optional[str]

    class Config:
        orm_mode = True


class PublicationCitation(BaseModel):
    publication_doi: str
    citation_doi: str

    class Config:
        orm_mode = True


class PublicationReference(BaseModel):
    publication_doi: str
    reference_doi: str

    class Config:
        orm_mode = True


class Source(BaseModel):
    id: int
    title: Optional[str]
    url: Optional[str]
    license: Optional[str]

    class Config:
        orm_mode = True


class PublicationSource(BaseModel):
    publication_doi: str
    source_id: int

    class Config:
        orm_mode = True


class Author(BaseModel):
    id: int
    name: Optional[str]
    normalized_name: Optional[str]

    class Config:
        orm_mode = True


class PublicationAuthor(BaseModel):
    publication_doi: str
    author_id: int

    class Config:
        orm_mode = True


class FieldOfStudy(BaseModel):
    id: int
    name: Optional[str]
    normalized_name: Optional[str]

    class Config:
        orm_mode = True


class PublicationFieldOfStudy(BaseModel):
    publication_doi: str
    field_of_study_id: int

    class Config:
        orm_mode = True


class PublicationNotFound(BaseModel):

    publication_doi: str
    last_try: Optional[datetime.datetime]
    pub_missing: Optional[str]

    class Config:
        orm_mode = True


class DiscussionData(BaseModel):

    id: int
    value: Optional[str]
    type: Optional[str]

    class Config:
        orm_mode = True


class DiscussionDataPoint(BaseModel):

    publication_doi: str
    discussion_data_point_id: int
    count: Optional[int]

    class Config:
        orm_mode = True


class DiscussionNewestSubj(BaseModel):

    id: int
    type: Optional[str]
    publication_doi: Optional[str]
    sub_id: Optional[str]
    created_at: Optional[datetime.datetime]
    score: Optional[float]
    bot_rating: Optional[float]
    followers: Optional[int]
    sentiment_raw: Optional[float]
    contains_abstract_raw: Optional[float]
    lang: Optional[str]
    location: Optional[str]
    source: Optional[str]
    subj_type: Optional[str]
    question_mark_count: Optional[int]
    exclamation_mark_count: Optional[int]
    length: Optional[int]
    entities: Optional[Json]

    class Config:
        orm_mode = True


class Trending(BaseModel):
    id: int
    publication_doi: Optional[str]
    duration: Optional[str]
    score: Optional[float]
    count: Optional[int]
    median_sentiment: Optional[float]
    sum_followers: Optional[int]
    abstract_difference: Optional[float]
    median_age: Optional[float]
    median_length: Optional[float]
    mean_questions: Optional[float]
    mean_exclamations: Optional[float]
    mean_bot_rating: Optional[float]
    projected_change: Optional[float]
    trending: Optional[float]
    ema: Optional[float]
    kama: Optional[float]
    ker: Optional[float]
    mean_score: Optional[float]
    stddev: Optional[float]

    class Config:
        orm_mode = True


class StatValue(BaseModel):
    value: Optional[str]
    name: Optional[str]

    class Config:
        orm_mode = True


class TimeValue(BaseModel):
    name: Optional[str]
    createdAt: Optional[datetime.datetime]

    class Config:
        orm_mode = True


class AmbaResponse(BaseModel):
    time: Optional[int]
    results: List[dict]
