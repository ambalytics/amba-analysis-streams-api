import logging

from typing import List
from sqlalchemy import text, bindparam
from sqlalchemy import asc, desc
from event_stream.models.model import Publication, PublicationAuthor
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session


def get_publications_db(session: Session, offset: int = 0, limit: int = 100, sort: str = 'id', order: str = 'asc') -> List[Publication]:
    if order != 'asc':
        order_by = desc(text(sort))
    else:
        order_by = asc(text(sort))
    return session.query(Publication).order_by(order_by).offset(offset).limit(limit).all()


# count all different publications
def get_publication_count(session: Session):
    s = text('SELECT COUNT(*) FROM (SELECT DISTINCT "publicationDoi" FROM "DiscussionData") AS temp;')
    return session.execute(s).fetchall()


def retrieve_publication(session: Session, doi):
    # todo check security
    pub = session.query(Publication).filter_by(doi=doi).all()
    a = text("""SELECT name FROM "PublicationAuthor" as p
                JOIN "Author" as a on (a.id = p."authorId")
                WHERE p."publicationDoi"=:doi""")

    params = {'doi': doi, }
    a = a.bindparams(bindparam('doi'))
    authors = session.execute(a, params).fetchall()

    f = text("""SELECT name FROM "PublicationFieldOfStudy" as p
                JOIN "FieldOfStudy" as a on (a.id = p."fieldOfStudyId")
                WHERE p."publicationDoi"=:doi""")
    f = f.bindparams(bindparam('doi'))
    fos = session.execute(f, params).fetchall()

    s = text("""SELECT id, title, url, license FROM "PublicationSource" as p
                JOIN "Source" as a on (a.id = p."sourceId")
                WHERE p."publicationDoi"=:doi""")
    s = s.bindparams(bindparam('doi'))
    sources = session.execute(s, params).fetchall()

    return {
        'publication': pub,
        'authors': authors,
        'fieldsOfStudy': fos,
        'sources': sources,
    }


def top_publications(session: Session, limit: int = 20):
    s = text("""SELECT
                      COUNT(d."id") as count, SUM(d."score") as score, SUM(d."questions") as question_mark_count,
                      SUM(d."exclamations") as exclamation_mark_count, SUM(d."followers") as followers,
                      AVG(d."length") as length_avg, AVG(d."abstractDifference") as contains_abstract_avg,
                      AVG(d."botScore") as bot_rating_avg, p.*
                        FROM "Publication" as p
                        JOIN "DiscussionData" as d ON (d."publicationDoi" = p."doi")
                        GROUP BY p."id"
                        ORDER BY score DESC
                        LIMIT :limit""")
    params = {'limit': limit, }
    s = s.bindparams(bindparam('limit'))
    return session.execute(s, params).fetchall()
