from enum import Enum
import datetime
from typing import Optional
from pydantic import BaseModel, Json


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
    level: Optional[int]

    class Config:
        orm_mode = True


class PublicationFieldOfStudy(BaseModel):
    publication_doi: str
    field_of_study_id: int

    class Config:
        orm_mode = True


class FieldOfStudyChildren(BaseModel):
    field_of_study_id: int
    child_field_of_study_id: int

    class Config:
        orm_mode = True


class DiscussionEntity(BaseModel):
    id: int
    entity: Optional[str]

    class Config:
        orm_mode = True


class DiscussionEntityData(BaseModel):
    publication_doi: str
    discussion_entity_id: int
    count: Optional[int]

    class Config:
        orm_mode = True


class DiscussionHashtag(BaseModel):
    id: int
    hashtag: Optional[str]

    class Config:
        orm_mode = True


class DiscussionHashtagData(BaseModel):
    publication_doi: str
    discussion_hashtag_id: int
    count: Optional[int]

    class Config:
        orm_mode = True


class DiscussionWord(BaseModel):
    id: int
    word: Optional[str]

    class Config:
        orm_mode = True


class DiscussionWordData(BaseModel):
    publication_doi: str
    discussion_word_id: int
    count: Optional[int]

    class Config:
        orm_mode = True


class DiscussionLocation(BaseModel):
    id: int
    location: Optional[str]

    class Config:
        orm_mode = True


class DiscussionLocationData(BaseModel):
    publication_doi: str
    discussion_location_id: int
    count: Optional[int]

    class Config:
        orm_mode = True


class DiscussionAuthor(BaseModel):
    id: int
    author: Optional[str]

    class Config:
        orm_mode = True


class DiscussionAuthorData(BaseModel):
    publication_doi: str
    discussion_author_id: int
    count: Optional[int]

    class Config:
        orm_mode = True


class DiscussionLang(BaseModel):
    id: int
    lang: Optional[str]

    class Config:
        orm_mode = True


class DiscussionLangData(BaseModel):
    publication_doi: str
    discussion_lang_id: int
    count: Optional[int]

    class Config:
        orm_mode = True


class DiscussionType(BaseModel):
    id: int
    type: Optional[str]

    class Config:
        orm_mode = True


class DiscussionTypeData(BaseModel):
    publication_doi: str
    discussion_type_id: int
    count: Optional[int]

    class Config:
        orm_mode = True


class DiscussionSource(BaseModel):
    id: int
    source: Optional[str]

    class Config:
        orm_mode = True


class DiscussionSourceData(BaseModel):
    publication_doi: str
    discussion_source_id: int
    count: Optional[int]

    class Config:
        orm_mode = True


class Trending(BaseModel):
    id: int
    publication_doi: Optional[str]
    duration: Optional[int]
    score: Optional[float]
    count: Optional[int]
    median_sentiment: Optional[float]
    sum_follower: Optional[int]
    abstract_difference: Optional[float]
    tweet_author_eveness: Optional[float]
    lang_eveness: Optional[float]
    location_eveness: Optional[float]
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
