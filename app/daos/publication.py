import logging
from datetime import timedelta

from typing import List
from sqlalchemy import text, bindparam
from sqlalchemy import asc, desc
from event_stream.models.model import Publication, PublicationAuthor
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session


def query_bottom(session, q, qs, qb, order, limit, offset, search):
    # only allow set string to avoid sql injection
    order_sql = ' ASC '
    if order == 'desc':
        order_sql = ' DESC '
    qb += order_sql

    qb += """
        LIMIT :limit OFFSET :offset
    """

    params = {'limit': limit, 'offset': offset}
    if len(search) > 3:
        params['search'] = '%' + search + '%'
        q += qs
        q += qb
        print(q)
        s = text(q).bindparams(bindparam('limit'), bindparam('offset'), bindparam('search'))
    else:
        q += qb
        print(q)
        s = text(q).bindparams(bindparam('limit'), bindparam('offset'))
    return session.execute(s, params).fetchall()


def get_publications(session: Session, offset: int = 0, limit: int = 10, sort: str = 'id', order: str = 'asc',
                     search: str = ''):
    q = """
        SELECT p.*, array_agg(a.id || ': ' || a.name), array_agg(fos.id || ': ' || fos.name) FROM publication p
            JOIN publication_author pa on p.doi = pa.publication_doi
            JOIN author as a on pa.author_id = a.id
            JOIN publication_field_of_study pfos on p.doi = pfos.publication_doi
            JOIN field_of_study fos on pfos.field_of_study_id = fos.id
    """

    qs = """
        WHERE p.title ILIKE :search
    """

    qb = ' GROUP BY p.id '
    sortable = ['id']
    if sort in sortable:
        if sort == 'id':
            qb += ' ORDER BY  '
            qb += 'p.id '
    else:
        qb += ' ORDER BY  '
        qb += 'p.id '
    return query_bottom(session, q, qs, qb, order, limit, offset, search)


def get_trending_publications(session: Session, offset: int = 0, limit: int = 10, sort: str = 'score',
                              order: str = 'desc', duration: str = "currently", search: str = ''):
    q = """
        SELECT ROW_NUMBER () OVER (ORDER BY score DESC) as trending_ranking, p.*, t.score, count, mean_sentiment, sum_followers, abstract_difference, mean_age, mean_length, 
        mean_questions, mean_exclamations, mean_bot_rating, projected_change, trending, ema, kama, ker, mean_score, 
        stddev, count(*) OVER() AS total_count  FROM trending t
        JOIN publication p on p.doi = t.publication_doi
        WHERE duration = :duration
        """

    qs = """
        AND p.title ILIKE :search
    """

    sortable = ['trending_ranking', 'score', 'count', 'mean_sentiment', 'sum_followers', 'abstract_difference',
                'mean_age', 'mean_length', 'mean_questions', 'mean_exclamations', 'mean_bot_rating',
                'projected_change', 'trending', 'ema', 'kama', 'ker', 'mean_score', 'stddev', 'year', 'citation_count']

    qb = ' ORDER BY  '
    if sort in sortable:
        qb += sort + ' '
    else:
        qb += 'score '
        # todo error

    order_sql = ' ASC '
    if order == 'desc':
        order_sql = ' DESC '
    qb += order_sql

    qb += """
            LIMIT :limit OFFSET :offset
        """

    params = {'duration': duration, 'limit': limit, 'offset': offset}
    if len(search) > 3:
        params['search'] = '%' + search + '%'
        q += qs
        q += qb
        # print(q)
        s = text(q).bindparams(bindparam('duration'), bindparam('limit'), bindparam('offset'), bindparam('search'))
    else:
        q += qb
        # print(q)
        s = text(q).bindparams(bindparam('duration'), bindparam('limit'), bindparam('offset'))
    return session.execute(s, params).fetchall()


def get_trending_publications_for_field_of_study(fos_id: int, session: Session, offset: int = 0, limit: int = 10,
                                                 sort: str = 'score',
                                                 order: str = 'desc', duration: str = "currently", search: str = ''):
    q = """
        SELECT ROW_NUMBER () OVER (ORDER BY score DESC) as trending_ranking, p.*, t.score, count, mean_sentiment, sum_followers, abstract_difference, mean_age, mean_length,
            mean_questions, mean_exclamations, mean_bot_rating, projected_change, trending, ema, kama, ker, mean_score,
            stddev, count(*) OVER() AS total_count 
        FROM trending t
            JOIN publication p on p.doi = t.publication_doi
            JOIN publication_field_of_study pfos on p.doi = pfos.publication_doi
            WHERE duration = :duration AND field_of_study_id = :fos_id 
        """

    qs = """
        AND p.title ILIKE :search
    """

    sortable = ['trending_ranking', 'score', 'count', 'mean_sentiment', 'sum_followers', 'abstract_difference',
                'mean_age', 'mean_length', 'mean_questions', 'mean_exclamations', 'mean_bot_rating',
                'projected_change', 'trending', 'ema', 'kama', 'ker', 'mean_score', 'stddev', 'year', 'citation_count']

    qb = ' ORDER BY  '
    if sort in sortable:
        qb += sort + ' '
    else:
        qb += 'score '

    order_sql = ' ASC '
    if order == 'desc':
        order_sql = ' DESC '
    qb += order_sql

    qb += """
            LIMIT :limit OFFSET :offset
        """

    params = {'duration': duration, 'limit': limit, 'offset': offset, 'fos_id': fos_id}
    if len(search) > 3:
        params['search'] = '%' + search + '%'
        q += qs
        q += qb
        # print(q)
        s = text(q).bindparams(bindparam('duration'), bindparam('limit'), bindparam('offset'), bindparam('fos_id'),
                               bindparam('search'))
    else:
        q += qb
        # print(q)
        s = text(q).bindparams(bindparam('duration'), bindparam('limit'), bindparam('offset'), bindparam('fos_id'))
    return session.execute(s, params).fetchall()


def get_trending_publications_for_author(author_id: int, session: Session, offset: int = 0, limit: int = 10,
                                         sort: str = 'score',
                                         order: str = 'desc', duration: str = "currently", search: str = ''):
    q = """
        SELECT ROW_NUMBER () OVER (ORDER BY score DESC) as trending_ranking, p.*, t.score, count, mean_sentiment, sum_followers, abstract_difference, mean_age, mean_length,
            mean_questions, mean_exclamations, mean_bot_rating, projected_change, trending, ema, kama, ker, mean_score,
            stddev
        FROM trending t
            JOIN publication p on p.doi = t.publication_doi
            JOIN publication_author pa on p.doi = pa.publication_doi
            WHERE duration = :duration AND author_id = :author_id 
        """

    qs = """
        AND p.title ILIKE :search
    """

    sortable = ['trending_ranking', 'score', 'count', 'mean_sentiment', 'sum_followers', 'abstract_difference',
                'mean_age', 'mean_length', 'mean_questions', 'mean_exclamations', 'mean_bot_rating',
                'projected_change', 'trending', 'ema', 'kama', 'ker', 'mean_score', 'stddev', 'year', 'citation_count']

    qb = ' ORDER BY  '
    if sort in sortable:
        qb += sort + ' '
    else:
        qb += 'score '

    order_sql = ' ASC '
    if order == 'desc':
        order_sql = ' DESC '
    qb += order_sql

    qb += """
            LIMIT :limit OFFSET :offset
        """

    params = {'duration': duration, 'limit': limit, 'offset': offset, 'author_id': author_id}
    if len(search) > 3:
        params['search'] = '%' + search + '%'
        q += qs
        q += qb
        # print(q)
        s = text(q).bindparams(bindparam('duration'), bindparam('limit'), bindparam('offset'), bindparam('author_id'),
                               bindparam('search'))
    else:
        q += qb
        # print(q)
        s = text(q).bindparams(bindparam('duration'), bindparam('limit'), bindparam('offset'), bindparam('author_id'))
    return session.execute(s, params).fetchall()


def retrieve_publication(session: Session, doi, duration: str = "currently"):
    pub = session.query(Publication).filter_by(doi=doi).all()
    a = text("""SELECT id, name FROM publication_author as p
                JOIN author as a on (a.id = p.author_id)
                WHERE p.publication_doi=:doi""")

    params = {'doi': doi, }
    a = a.bindparams(bindparam('doi'))
    authors = session.execute(a, params).fetchall()

    f = text("""SELECT id, name FROM publication_field_of_study as p
                JOIN field_of_study as a on (a.id = p.field_of_study_id)
                WHERE p.publication_doi=:doi""")
    f = f.bindparams(bindparam('doi'))
    fos = session.execute(f, params).fetchall()

    s = text("""SELECT id, title, url, license FROM publication_source as p
                JOIN source as a on (a.id = p.source_id)
                WHERE p.publication_doi=:doi""")
    s = s.bindparams(bindparam('doi'))
    sources = session.execute(s, params).fetchall()

    query = """
        SELECT trending_ranking
            FROM (SELECT ROW_NUMBER() OVER (ORDER BY score DESC) as trending_ranking, publication_doi FROM trending t
        WHERE duration = :duration) t
            WHERE publication_doi = :doi
    """
    params = {'duration': duration, 'doi': doi}
    s = text(query).bindparams(bindparam('duration'), bindparam('doi'))
    rank = session.execute(s, params).fetchone()

    if rank:
        r = rank[0]
    else:
        r = None

    return {
        'publication': pub,
        'authors': authors,
        'fields_of_study': fos,
        'sources': sources,
        'trending_ranking': r
    }


# remove todo
def get_trending_publication2s(session: Session, limit: int = 20):
    s = text("""
        SELECT p.*, t.score, count, mean_sentiment, sum_follower, abstract_difference, tweet_author_diversity, lan_diversity, location_diversity, mean_age, mean_length, avg_questions, avg_exclamations, projected_change FROM trending t
        JOIN publication p on p.doi = t.publication_doi
        ORDER BY score DESC
                        LIMIT :limit""")
    params = {'limit': limit, }
    s = s.bindparams(bindparam('limit'))
    return session.execute(s, params).fetchall()

    trending_publications = """
    SELECT p.*, t.score, count, mean_sentiment, sum_follower, abstract_difference, tweet_author_diversity, lan_diversity, location_diversity, mean_age, mean_length, avg_questions, avg_exclamations, projected_change FROM trending t
    JOIN publication p on p.doi = t.publication_doi
    ORDER BY score DESC
    LIMIT 10
    """

    authors_with_publications = """
    SELECT * FROM author a
     JOIN publication_author pa on pa.author_id = a.id
     JOIN publication p on p.doi = pa.publication_doi
    LIMIT 10
    """

    fos_with_publications = """
    SELECT * FROM field_of_study fos
     JOIN publication_field_of_study pfos on pfos.field_of_study_id = fos.id
     JOIN publication p on p.doi = pfos.publication_doi
    LIMIT 10
    """
