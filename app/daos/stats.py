from datetime import timedelta

from sqlalchemy import text, bindparam
from sqlalchemy.orm import Session  # type: ignore


def get_types(session: Session, doi, **kwargs):
    query = """SELECT "type" as "value", SUM(count) as count FROM discussion_type_data d
                JOIN discussion_type t on t.id = d.discussion_type_id
             """

    if doi:
        query += 'WHERE publication_doi=:doi '
        query += 'GROUP BY "type" ORDER BY count DESC '
        params = {'doi': doi, }
        s = text(query)
        s = s.bindparams(bindparam('doi'))
        return session.execute(s, params).fetchall()
    s = text(query + 'GROUP BY "type" ORDER BY count DESC ')
    return session.execute(s).fetchall()


def get_sentiment(query_api, doi):
    params = {
        '_start': timedelta(days=-30),
    }

    if doi:
        params['_doi'] = doi
        query = """
            from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "sentiment_raw")
              |> filter(fn: (r) => r["doi"] == _doi)
              |> keep(columns: ["doi", "_value"])
              |> group()
              |> median()
              |> rename(columns: {_value: "count"})
        """
    else:
        query = """
            from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "sentiment_raw")
              |> keep(columns: ["doi", "_value"])
              |> group()
              |> median()
              |> rename(columns: {_value: "count"})
        """
    tables = query_api.query(query, params=params)
    result = 0
    for table in tables:
        for record in table.records:
            result = record['count']
    return result


#def get_sources(session: Session, doi, limit: int = 10, min_percentage: float = 1):
# todo save in influx and than query


def get_words(session: Session, doi, limit: int = 100, **kwargs):
    # add percentage or something to allow world word cloud
    query = """SELECT SUM(dwd.count) as count, dw.word as value FROM discussion_word_data as dwd
                    JOIN discussion_word as dw ON (dwd.discussion_word_id = dw.id)  """
    extra = """ GROUP BY dw.id ORDER BY count DESC LIMIT :limit """

    if doi:
        query += 'WHERE publication_doi=:doi '
        query += extra
        params = {'doi': doi, 'limit': limit, }
        s = text(query)
        s = s.bindparams(bindparam('doi'), bindparam('limit'))
        return session.execute(s, params).fetchall()

    params = {'limit': limit, }
    print(query + extra)
    s = text(query + extra)
    s = s.bindparams(bindparam('limit'))
    return session.execute(s, params).fetchall()


def get_top_lang(query_api, doi):
    params = {
        '_start': timedelta(days=-30),
    }

    if doi:
        params['_doi'] = doi
        query = """
            from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "lang")
              |> filter(fn:(r) => r._value != "und")
              |> filter(fn: (r) => r["doi"] == _doi)
              |> duplicate(column: "_value", as: "language")
              |> group(columns: ["language"])
              |> count()
              |> group()
              |> sort(desc: true)
              |> rename(columns: {_value: "count"})
              |> rename(columns: {language: "value"})
        """
    else:
        query = """
            from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "lang")
              |> filter(fn:(r) => r._value != "und")
              |> duplicate(column: "_value", as: "language")
              |> group(columns: ["language"])
              |> count()
              |> group()
              |> sort(desc: true)
              |> rename(columns: {_value: "count"})
              |> rename(columns: {language: "value"})
        """
    print(params)
    tables = query_api.query(query, params=params)
    results = []
    for table in tables:
        for record in table.records:
            results.append({'value': record['value'], 'count': record['count']})
    return results


def get_top_entities(session: Session, doi, limit: int = 20, min_percentage: float = 1):
    query = """WITH result AS  
              (  
                 (  
                    SELECT entity, SUM(count) as count, 
                     ROUND(COUNT(*) / CAST( SUM(count(*)) OVER () AS FLOAT) * 100) as p  
                    FROM discussion_entity_data as dwd
                    JOIN discussion_entity as dw ON (dwd.discussion_entity_id  = dw.id)
                    """

    if doi:
        query += """WHERE dwd.publication_doi=:doi
                   GROUP BY "entity"
                            ORDER BY count DESC  
                            LIMIT :limit)			  
                          UNION  
                          (  
                             SELECT 'total' as entity, COUNT(*) as c, 100 as p  
                                FROM discussion_entity_data d
                              WHERE d.publication_doi=:doi      
                           )	  
                       )  
                        SELECT "entity" as value, count FROM result WHERE result.p >= :mp ORDER BY count DESC """
        params = {'doi': doi, 'limit': limit, 'mp': min_percentage, }
        s = text(query)
        s = s.bindparams(bindparam('doi'), bindparam('limit'), bindparam('mp'))
        return session.execute(s, params).fetchall()

    query += """  GROUP BY "entity"
                    ORDER BY count DESC  
                    LIMIT :limit)			  
                  UNION  
                  (  
                     SELECT 'total' as entity, COUNT(*) as c, 100 as p  
                        FROM discussion_entity_data d
                   )	  
               )  
                SELECT "entity" as value, count FROM result WHERE result.p >= :mp ORDER BY count DESC """

    params = {'limit': limit, 'mp': min_percentage, }
    s = text(query)
    s = s.bindparams(bindparam('limit'), bindparam('mp'))
    return session.execute(s, params).fetchall()


def get_top_hashtags(session: Session, doi, limit: int = 10, min_percentage: float = 1):
    query = """WITH result AS  
                  (  
                     (  
                    SELECT hashtag, SUM(count) as c, 
                     ROUND(COUNT(*) / CAST( SUM(count(*)) OVER () AS FLOAT) * 100) as p  
                    FROM discussion_hashtag_data as dwd
                    JOIN discussion_hashtag as dw ON (dwd.discussion_hashtag_id  = dw.id)        """
    extra1 = """   GROUP BY hashtag
                         ORDER BY c DESC  
                         LIMIT :limit
                      )			  
                      UNION  
                      (  
                         SELECT 'total' as hashtag, COUNT(*) as c, 100 as p  
                        FROM discussion_hashtag_data d """
    extra2 = """ )	  
                   )  
                    SELECT "hashtag" as value, c as count FROM result WHERE result.p >= :mp ORDER BY count DESC """

    if doi:
        doiSelector = ' WHERE publication_doi=:doi'
        query += doiSelector
        query += extra1
        query += doiSelector
        query += extra2
        params = {'doi': doi, 'limit': limit, 'mp': min_percentage, }
        s = text(query)
        s = s.bindparams(bindparam('doi'), bindparam('limit'), bindparam('mp'))
        return session.execute(s, params).fetchall()

    params = {'limit': limit, 'mp': min_percentage, }
    s = text(query + extra1 + extra2)
    s = s.bindparams(bindparam('limit'), bindparam('mp'))
    return session.execute(s, params).fetchall()


# get list of user countries
def get_country_list(query_api, doi):
    params = {
        '_start': timedelta(days=-30),
    }

    if doi:
        params['_doi'] = doi

        query = """
            from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "author_location")
              |> filter(fn:(r) => r._value != "unknown")
              |> filter(fn: (r) => r["doi"] == _doi)
              |> duplicate(column: "_value", as: "author_location")
              |> group(columns: ["author_location"])
              |> count()
              |> group()
              |> sort(desc: true)
              |> rename(columns: {_value: "count"})
              |> rename(columns: {author_location: "location"})
        """
    else:
        query = """
            from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "author_location")
              |> filter(fn:(r) => r._value != "unknown")
              |> duplicate(column: "_value", as: "author_location")
              |> group(columns: ["author_location"])
              |> count()
              |> group()
              |> sort(desc: true)
              |> rename(columns: {_value: "count"})
              |> rename(columns: {author_location: "location"})
        """
    tables = query_api.query(query, params=params)
    results = []
    for table in tables:
        # print(table)
        for record in table.records:
            results.append({'location': record['location'], 'count': record['count']})
    return results


# get number of different tweet authors
def get_tweet_author_count(query_api, doi):
    params = {
        '_start': timedelta(days=-30),
    }

    if doi:
        params['_doi'] = doi
        query = """
            from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "author_name")
              |> filter(fn: (r) => r["doi"] == _doi)
              |> keep(columns: ["_value"])
              |> distinct()
              |> count()
              |> group()
              |> rename(columns: {_value: "count"})
        """
    else:
        query = """
            from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "author_name")
              |> keep(columns: ["_value"])
              |> distinct()
              |> count()
              |> group()
              |> rename(columns: {_value: "count"})
        """
    tables = query_api.query(query, params=params)
    result = 0
    for table in tables:
        for record in table.records:
            result = record['count']
    return result


# get number of different tweet authors
def get_tweet_count(query_api, doi):
    params = {
        '_start': timedelta(days=-30),
    }

    if doi:
        params['_doi'] = doi
        return get_count(query_api, params)

    return get_count(query_api, params, False)


# get total sum of followers reached
def get_followers_reached(query_api, doi):
    params = {
        '_start': timedelta(days=-30),
        '_fieldSum': 'followers'
    }

    if doi:
        params['_doi'] = doi
        return get_sum(query_api, params)

    return get_sum(query_api, params, False)

def get_count(query_api, p, with_doi = True):
    if with_doi:
        query = """
            from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "score")
              |> filter(fn: (r) => r["doi"] == _doi)
              |> keep(columns: ["_value"])
              |> count()
              |> group()
              |> rename(columns: {_value: "count"})
        """
    else:
        query = """
            from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == "score")
              |> keep(columns: ["_value"])
              |> count()
              |> group()
              |> rename(columns: {_value: "count"})
        """
    tables = query_api.query(query, params=p)
    result = 0
    for table in tables:
        for record in table.records:
            result = record['count']
    return result

def get_sum(query_api, p, with_doi = True):
    if with_doi:
        query = """
            from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == _fieldSum)
              |> filter(fn: (r) => r["doi"] == _doi)
              |> keep(columns: ["_value"])
              |> sum()
              |> group()
              |> rename(columns: {_value: "sum"})
        """
    else:
        query = """
            from(bucket: "trending")
              |> range(start: _start)
              |> filter(fn: (r) => r["_measurement"] == "trending")
              |> filter(fn: (r) => r["_field"] == _fieldSum)
              |> keep(columns: ["_value"])
              |> sum()
              |> group()
              |> rename(columns: {_value: "sum"})
        """
    tables = query_api.query(query, params=p)
    result = 0
    for table in tables:
        for record in table.records:
            result = record['sum']
    return result

# get average score
def get_total_score(query_api, doi):
    params = {
        '_start': timedelta(days=-30),
        '_fieldSum': 'score'
    }

    if doi:
        params['_doi'] = doi
        return get_sum(query_api, params)

    return get_sum(query_api, params, False)


# get tweets binned every hour
def get_time_count_binned_summed(query_api, doi):
    p = {"_start":timedelta(hours=-24),
         "_bucket": 'trending',
         }

    if doi:
        p['_doi'] = doi
        query = """
        from(bucket: "trending")
          |> range(start: _start)
          |> filter(fn: (r) => r["_measurement"] == "trending")
          |> filter(fn: (r) => r["_field"] == "score")
          |> filter(fn: (r) => r["doi"] == _doi)
          |> aggregateWindow(every: 10m, fn: count, createEmpty: true)
          |> cumulativeSum(columns: ["_value"])
          |> keep(columns: ["_time", "_value"])
          |> rename(columns: {_value: "count"})
          |> rename(columns: {_time: "time"})
        """
    else:
        query = """
        from(bucket: "trending")
          |> range(start: _start)
          |> filter(fn: (r) => r["_measurement"] == "trending")
          |> filter(fn: (r) => r["_field"] == "score")
          |> keep(columns: ["_time", "_value"])
          |> aggregateWindow(every: 10m, fn: count, createEmpty: true)
          |> cumulativeSum(columns: ["_value"])
          |> rename(columns: {_value: "count"})
          |> rename(columns: {_time: "time"})
        """

    tables = query_api.query(query, params=p)
    results = []
    for table in tables:
        # print(table)
        for record in table.records:
            results.append({'time': record['time'], 'count': record['count']})
    return results


def get_time_count_binned(query_api, doi):
    p = {"_start":timedelta(hours=-24),
         "_bucket": 'trending',
         }
    if doi:
        p['_doi'] = doi
        query = """
        from(bucket: "trending")
          |> range(start: _start)
          |> filter(fn: (r) => r["_measurement"] == "trending")
          |> filter(fn: (r) => r["_field"] == "score")
          |> filter(fn: (r) => r["doi"] == _doi)
          |> aggregateWindow(every: 1h, fn: count, createEmpty: true)
          |> keep(columns: ["_time", "_value"])
          |> rename(columns: {_value: "count"})
          |> rename(columns: {_time: "time"})
        """
    else:
        query = """
       from(bucket: "trending")
         |> range(start: _start)
         |> filter(fn: (r) => r["_measurement"] == "trending")
         |> filter(fn: (r) => r["_field"] == "score")
         |> keep(columns: ["_time", "_value"])
         |> aggregateWindow(every: 1h, fn: count, createEmpty: true)
         |> rename(columns: {_value: "count"})
         |> rename(columns: {_time: "time"})
       """
    tables = query_api.query(query, params=p)
    results = []
    for table in tables:
        # print(table)
        for record in table.records:
            results.append({'time': record['time'], 'count': record['count']})
    return results


# get hour binned periodic count
# def get_tweet_time_of_day(session: Session, doi):
#     query = """SELECT COUNT(*) / COUNT(distinct DATE("createdAt")) as count,
#                 date_part('hour', "createdAt") AS "value" FROM "DiscussionData"
#                 """
#     if doi:
#         query += ' WHERE publication_doi=:doi GROUP BY "value"'
#         params = {'doi': doi, }
#         s = text(query)
#         s = s.bindparams(bindparam('doi'))
#         return session.execute(s, params).fetchall()
#     s = text(query + 'GROUP BY "value"')
#     return session.execute(s).fetchall()


def fetch_with_doi_filter(session: Session, query, doi):
    if doi:
        query += 'WHERE publication_doi=:doi'
        params = {'doi': doi, }
        s = text(query)
        s = s.bindparams(bindparam('doi'))
        return session.execute(s, params).fetchall()
    s = text(query)
    return session.execute(s).fetchall()
