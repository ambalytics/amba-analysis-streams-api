from enum import Enum
import datetime
from typing import Optional
from pydantic import BaseModel


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
    pubDate: Optional[str]
    year: Optional[int]
    publisher: Optional[str]
    citationCount: Optional[int]
    title: Optional[str]
    normalizedTitle: Optional[str]
    abstract: Optional[str]

    class Config:
        orm_mode = True


class Source(BaseModel):
    id: int
    title: Optional[str]
    url: Optional[str]
    license: Optional[str]

    class Config:
        orm_mode = True


class FieldOfStudy(BaseModel):
    id: int
    name: Optional[str]
    normalizedName: Optional[str]
    level: Optional[int]

    class Config:
        orm_mode = True


class Author(BaseModel):
    id: int
    name: Optional[str]
    normalizedName: Optional[str]

    class Config:
        orm_mode = True


class PublicationCitation(BaseModel):
    publicationDoi: str
    citationId: str


class PublicationReference(BaseModel):
    publicationDoi: str
    referenceId: str


class PublicationFieldOfStudy(BaseModel):
    publicationDoi: str
    fieldOfStudyId: int


class PublicationAuthor(BaseModel):
    publicationDoi: str
    authorId: int


class PublicationSource(BaseModel):
    publicationDoi: str
    sourceId: int


class DiscussionData(BaseModel):
    id: int
    publicationDoi: Optional[str]
    createdAt: Optional[datetime.datetime]
    score: Optional[float]
    timeScore: Optional[float]
    typeScore: Optional[float]
    userScore: Optional[float]
    language: Optional[str]
    source: Optional[str]
    abstractDifference: Optional[float]
    length: Optional[int]
    questions: Optional[int]
    exclamations: Optional[int]
    type: Optional[str]
    sentiment: Optional[float]
    subjId: Optional[int]
    followers: Optional[int]
    botScore: Optional[float]
    authorName: Optional[str]
    authorLocation: Optional[str]
    sourceId: Optional[str]

    class Config:
        orm_mode = True


class DiscussionEntity(BaseModel):
    id: int
    entity: Optional[str]


class DiscussionHashtag(BaseModel):
    id: int
    hashtag: Optional[str]


class DiscussionWord(BaseModel):
    id: int
    word: Optional[str]


class DiscussionAuthor(BaseModel):
    id: int
    name: Optional[str]


class DiscussionEntityData(BaseModel):
    discussionDataId: int
    discussionEntityId: int


class DiscussionAuthorData(BaseModel):
    discussionDataId: int
    discussionAuthorId: int


class DiscussionWordData(BaseModel):
    discussionDataId: int
    discussionWordId: int
    count: Optional[int]


class DiscussionHashtagData(BaseModel):
    discussionDataId: int
    discussionHashtagId: int


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
