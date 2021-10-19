from sqlalchemy import text, bindparam
from sqlalchemy.orm import Session


def retrieve_field_of_study(session: Session, id, with_pubs=False):
    """ retrieve field of study from postgres """
    params = {'id': id}
    f = text("""SELECT name FROM field_of_study WHERE id=:id""")
    f = f.bindparams(bindparam('id'))
    fos = session.execute(f, params).fetchall()

    result = {'fields_of_study': fos[0]}

    if with_pubs:
        p = text("""
            SELECT p.* FROM publication_field_of_study pf
            JOIN publication p on p.doi = pf.publication_doi 
            WHERE pf.field_of_study_id=:id""")
        p = p.bindparams(bindparam('id'))
        pubs = session.execute(p, params).fetchall()

        result['publications'] = pubs

    return result


def get_fields_of_study(session: Session, offset: int = 0, limit: int = 10, sort: str = 'id', order: str = 'asc',
                        search: str = ''):
    """ retrieve fields of study from postgres """
    q = """
         SELECT fos.*, array_agg('{name: "' || p.title || '", doi: "' || p.doi || '", date: "' || p.pub_date || '"}') FROM field_of_study fos
             JOIN publication_field_of_study pfos on pfos.field_of_study_id = fos.id
             JOIN publication p on p.doi = pfos.publication_doi
    """

    qs = """
        WHERE fos.name ILIKE '%:search%'
    """

    qb = '  GROUP BY fos.id ORDER BY  '
    sortable = ['id']
    if sort in sortable:
        if sort == 'id':
            qb += 'fos.id '
    else:
        qb += 'fos.id '

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


def get_trending_fields_of_study(session: Session, offset: int = 0, limit: int = 10, sort: str = 'score',
                                 order: str = 'desc', search: str = '', duration: str = 'currently'):
    """ retrieve field of study from postgres """
    q = """
        SELECT ROW_NUMBER () OVER (ORDER BY score DESC) as trending_ranking, *, count(*) OVER() AS total_count FROM (
            SELECT fos.id, fos.name, count(t.publication_doi) as pub_count,
                SUM(t.score) as score, SUM(count) as count, AVG(mean_sentiment) as mean_sentiment,
                SUM(sum_followers) as sum_followers, AVG(abstract_difference) as abstract_difference,
                AVG(mean_age) as mean_age, AVG(mean_length) as mean_length, AVG(mean_questions) as mean_questions,
                AVG(mean_exclamations) as mean_exclamations, AVG(mean_bot_rating) as mean_bot_rating,
                AVG(projected_change) as projected_change, AVG(trending) as trending, AVG(ema) as ema, AVG(kama) as kama,
                AVG(ker) as ker, AVG(mean_score) as mean_score, AVG(stddev) as stddev
            FROM trending t
                JOIN publication p on p.doi = t.publication_doi
                JOIN publication_field_of_study pfos on p.doi = pfos.publication_doi
                JOIN field_of_study fos on fos.id = pfos.field_of_study_id
                WHERE duration = :duration 
        """

    sortable = ['trending_ranking', 'score', 'count', 'mean_sentiment', 'sum_followers', 'abstract_difference',
                'mean_age', 'mean_length', 'mean_questions', 'mean_exclamations', 'mean_bot_rating',
                'projected_change', 'trending', 'ema', 'kama', 'ker', 'mean_score', 'stddev', 'pub_count']

    qb = '  GROUP BY fos.id) t ORDER BY  '
    qs = """
            AND fos.name ILIKE :search
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
