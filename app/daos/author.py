import logging

from typing import List
from sqlalchemy import text, bindparam
from sqlalchemy import asc, desc
from event_stream.models.model import Publication, PublicationAuthor
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session


def retrieve_author(session: Session, id, with_pubs=False):
    """
    get author data for from postresql

    - **id**: author id
    - **with_pubs**: include publication
    """
    params = {'id': id}
    f = text("""SELECT name FROM author WHERE id=:id""")
    f = f.bindparams(bindparam('id'))
    a = session.execute(f, params).fetchall()

    result = {'author': a[0]}

    if with_pubs:
        p = text("""
              SELECT p.* FROM publication_author pf
              JOIN publication p on p.doi = pf.publication_doi 
              WHERE pf.author_id=:id""")
        p = p.bindparams(bindparam('id'))
        pubs = session.execute(p, params).fetchall()

        result['publications'] = pubs

    return result


def get_authors(session: Session, offset: int = 0, limit: int = 10, sort: str = 'id', order: str = 'asc',
                     search: str = ''):
    """
    get authors from postresql
    """
    q = """
         SELECT a.*, array_agg('{name: "' || p.title || '", doi: "' || p.doi || '", date: "' || p.pub_date || '"}') FROM author a
             JOIN publication_author pfos on pfos.author_id = a.id
             JOIN publication p on p.doi = pfos.publication_doi
    """

    qs = """
        WHERE a.name ILIKE '%:search%'
    """

    qb = '  GROUP BY a.id ORDER BY  '
    sortable = ['id']
    if sort in sortable:
        if sort == 'id':
            qb += 'a.id '
    else:
        qb += 'a.id '

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
        # print(q)
        s = text(q).bindparams(bindparam('limit'), bindparam('offset'), bindparam('search'))
    else:
        q += qb
        # print(q)
        s = text(q).bindparams(bindparam('limit'), bindparam('offset'))
    return session.execute(s, params).fetchall()


def get_trending_authors(session: Session, offset: int = 0, limit: int = 10, sort: str = 'score', order: str = 'asc',
                         search: str = '', duration: str = "currently"):
    """ get trending authors from postgresql
    """
    q = """
    SELECT ROW_NUMBER () OVER (ORDER BY score DESC) as trending_ranking, *, count(*) OVER() AS total_count FROM (
          SELECT a.id, a.name, count(t.publication_doi) as pub_count,
              SUM(t.score) as score, SUM(count) as count, AVG(mean_sentiment) as mean_sentiment,
              SUM(sum_followers) as sum_followers, AVG(abstract_difference) as abstract_difference,
              AVG(mean_age) as mean_age, AVG(mean_length) as mean_length, AVG(mean_questions) as mean_questions,
              AVG(mean_exclamations) as mean_exclamations, AVG(mean_bot_rating) as mean_bot_rating,
              AVG(projected_change) as projected_change, AVG(trending) as trending, AVG(ema) as ema, AVG(kama) as kama,
              AVG(ker) as ker, AVG(mean_score) as mean_score, AVG(stddev) as stddev
          FROM trending t
              JOIN publication p on p.doi = t.publication_doi
              JOIN publication_author pa on p.doi = pa.publication_doi
              JOIN author a on a.id = pa.author_id
          WHERE duration = :duration
      """

    sortable = ['trending_ranking', 'score', 'count', 'mean_sentiment', 'sum_followers', 'abstract_difference',
                'mean_age', 'mean_length', 'mean_questions', 'mean_exclamations', 'mean_bot_rating',
                'projected_change', 'trending', 'ema', 'kama', 'ker', 'mean_score', 'stddev', 'pub_count']

    qb = '  GROUP BY a.id) t ORDER BY  '
    qs = """
            AND a.name ILIKE :search
        """
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
