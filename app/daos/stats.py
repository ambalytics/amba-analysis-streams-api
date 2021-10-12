import json
import time
from datetime import timedelta

from sqlalchemy import text, bindparam
from sqlalchemy.orm import Session  # type: ignore


def get_tweet_author_count(doi, session: Session, id, mode="publication"):
    params = {}
    if mode == "fieldOfStudy":
        query = """SELECT SUM(count) as count 
                    FROM discussion_data_point as ddp
                         JOIN discussion_data as dd ON (ddp.discussion_data_point_id = dd.id)
                         JOIN publication_field_of_study as pfos on ddp.publication_doi = pfos.publication_doi
                    WHERE type = 'name' AND pfos.field_of_study_id=:id """
        params['id'] = id
    elif mode == "author":
        query = """SELECT SUM(count) as count 
                    FROM discussion_data_point as ddp
                         JOIN discussion_data as dd ON (ddp.discussion_data_point_id = dd.id)
                         JOIN publication_author as pfos on ddp.publication_doi = pfos.publication_doi
                    WHERE type = 'name'  AND pfos.author_id=:id """
        params['id'] = id
    else:  # default publication
        query = """SELECT SUM(count) as count
                    FROM discussion_data_point as ddp
                         JOIN discussion_data as dd ON (ddp.discussion_data_point_id = dd.id)
                    WHERE type = 'name'  """

    if doi:
        query += " AND publication_doi = :doi "
        params['doi'] = doi

    # print(query)
    # print(params)

    s = text(query)
    if doi:
        s = s.bindparams(bindparam('doi'))

    return session.execute(s, params).fetchall()


def get_tweets(doi, session: Session, id, mode="publication"):
    params = {}
    query = """  
         SELECT p.*, p.title, p.abstract, discussion_newest_subj.*
            FROM discussion_newest_subj
                     JOIN publication p on discussion_newest_subj.publication_doi = p.doi
          """
    if mode == "publication":
        query += """ WHERE discussion_newest_subj.type = 'twitter' """
    if mode == "fieldOfStudy":
        query += """
                JOIN publication_field_of_study pfos on discussion_newest_subj.publication_doi = pfos.publication_doi
                  WHERE discussion_newest_subj.type = 'twitter' AND pfos.field_of_study_id = :id
                """
        params['id'] = id
    if mode == "author":
        query += """
                JOIN publication_author pfos on discussion_newest_subj.publication_doi = pfos.publication_doi
                  WHERE discussion_newest_subj.type = 'twitter' AND pfos.author_id = :id
                """
        params['id'] = id
    extra = """              
            ORDER BY created_at DESC
            LIMIT 1;    
    """

    if doi:
        query += " AND publication_doi = :doi "
        params['doi'] = doi

    query += extra

    # print(query)
    # print(params)

    s = text(query)
    if doi:
        s = s.bindparams(bindparam('doi'))
    if id:
        s = s.bindparams(bindparam('id'))

    tweet_data = session.execute(s, params).fetchone()

    a = text("""SELECT id, name FROM publication_author as p
                    JOIN author as a on (a.id = p.author_id)
                    WHERE p.publication_doi=:doi""")

    params = {'doi': tweet_data['doi'], }
    a = a.bindparams(bindparam('doi'))
    authors = session.execute(a, params).fetchall()

    return {'authors': authors, 'data': tweet_data}


def get_dois_for_field_of_study(id, session: Session, duration="currently"):
    query = """
        SELECT t.publication_doi
        FROM trending t
            JOIN publication_field_of_study pfos on t.publication_doi = pfos.publication_doi
        WHERE duration = :duration AND field_of_study_id = :fos_id;
    """

    params = {'duration': duration, 'fos_id': id}
    s = text(query)
    s = s.bindparams(bindparam('duration'), bindparam('fos_id'))
    rows = session.execute(s, params).fetchall()

    result = []
    for r in rows:
        result.append(r[0])

    # print(result)
    return result


def get_dois_for_author(id, session: Session, duration="currently"):
    query = """
        SELECT t.publication_doi
        FROM trending t
            JOIN publication_author pa on t.publication_doi = pa.publication_doi
        WHERE duration = :duration AND pa.author_id = :author_id;
    """

    params = {'duration': duration, 'author_id': id}
    s = text(query)
    s = s.bindparams(bindparam('duration'), bindparam('author_id'))
    rows = session.execute(s, params).fetchall()

    result = []
    for r in rows:
        result.append(r[0])

    # print(result)
    return result


def get_profile_information_avg(session: Session, duration="currently"):
    query = """
        SELECT 'mean_score' as type, MIN(mean_score), MAX(mean_score), AVG(mean_score)
        FROM trending
        WHERE duration = :duration
        UNION
        SELECT 'mean_bot_rating' as type, MIN(mean_bot_rating), MAX(mean_bot_rating), AVG(mean_bot_rating)
        FROM trending
        WHERE duration = :duration
        UNION
        SELECT 'median_sentiment' as type, MIN(median_sentiment), MAX(median_sentiment), AVG(median_sentiment)
        FROM trending
        WHERE duration = :duration
        UNION
        SELECT 'sum_followers' as type, MIN(sum_followers), MAX(sum_followers), AVG(sum_followers)
        FROM trending
        WHERE duration = :duration
        UNION
        SELECT 'abstract_difference' as type, MIN(abstract_difference), MAX(abstract_difference), AVG(abstract_difference)
        FROM trending
        WHERE duration = :duration
        UNION
        SELECT 'mean_questions' as type, MIN(mean_questions), MAX(mean_questions), AVG(mean_questions)
        FROM trending
        WHERE duration = :duration
        UNION
        SELECT 'mean_exclamations' as type, MIN(mean_exclamations), MAX(mean_exclamations), AVG(mean_exclamations)
        FROM trending
        WHERE duration = :duration
        UNION
        SELECT 'median_length' as type, MIN(median_length), MAX(median_length), AVG(median_length)
        FROM trending
        WHERE duration = :duration;
    """

    params = {'duration': duration, }
    s = text(query)
    s = s.bindparams(bindparam('duration'))
    rows = session.execute(s, params).fetchall()
    result = {'min': {}, 'max': {}, 'avg': {}}
    for r in rows:
        result['min'][r[0]] = r[1]
        result['max'][r[0]] = r[2]
        result['avg'][r[0]] = r[3]

    return result


def get_discussion_data_list_with_percentage(session: Session, doi, limit: int = 20, min_percentage: float = 1,
                                             dd_type="lang"):
    query = """
            WITH result AS
                     (
                         (
                             SELECT "value",
                                    SUM(count)                                                             as c,
                                    ROUND(SUM(count) / CAST(SUM(SUM(count)) OVER () AS FLOAT) * 1000) / 10 as p
                             FROM (SELECT "value", "count"
                                   FROM discussion_data_point as ddp
                                            JOIN discussion_data as dd ON (ddp.discussion_data_point_id = dd.id)
                                   WHERE type = :type and value != 'und' and value != 'unknown'
                                     """
    extra1 = """) temp
                             GROUP BY "value"
                             ORDER BY c DESC
                             LIMIT :limit
                         )
                         UNION
                         (
                             SELECT 'total' as "value", SUM(count) as c, 100 as p
                             FROM discussion_data_point as ddp
                                      JOIN discussion_data as dd ON (ddp.discussion_data_point_id = dd.id)
                             WHERE type = :type and value != 'und' and value != 'unknown'
                               """
    extra2 = """
                         )
                     )
            SELECT "value", c as count, p
            FROM result
            WHERE result.p >= :mp
            ORDER BY count DESC;
        """
    params = {
        'type': dd_type,
        'limit': limit,
        'mp': min_percentage
    }

    if doi:
        query += ' AND publication_doi=:doi '
        params['doi'] = doi

    query += extra1

    if doi:
        query += ' AND publication_doi=:doi '

    query += extra2
    s = text(query)
    # print(query)
    # print(params)

    if 'doi' in params:
        s = s.bindparams(bindparam('type'), bindparam('limit'), bindparam('mp'), bindparam('doi'))
    else:
        s = s.bindparams(bindparam('type'), bindparam('limit'), bindparam('mp'))

    return session.execute(s, params).fetchall()


def get_discussion_data_list(session: Session, doi, limit, id, mode="publication", dd_type="word"):
    params = {'type': dd_type}
    if mode == "fieldOfStudy":
        query = """SELECT SUM(ddp.count) as count, dd.value
                    FROM discussion_data_point as ddp
                         JOIN discussion_data as dd ON (ddp.discussion_data_point_id = dd.id)
                         JOIN publication_field_of_study as pfos on ddp.publication_doi = pfos.publication_doi
                    WHERE type = :type and value != 'und' and value != 'unknown' AND pfos.field_of_study_id=:id """
        params['id'] = id
    elif mode == "author":
        query = """SELECT SUM(ddp.count) as count, dd.value
                    FROM discussion_data_point as ddp
                         JOIN discussion_data as dd ON (ddp.discussion_data_point_id = dd.id)
                         JOIN publication_author as pfos on ddp.publication_doi = pfos.publication_doi
                    WHERE type = :type and value != 'und' and value != 'unknown' AND pfos.author_id=:id """
        params['id'] = id
    else:  # default publication
        query = """SELECT SUM(ddp.count) as count, dd.value
                    FROM discussion_data_point as ddp
                         JOIN discussion_data as dd ON (ddp.discussion_data_point_id = dd.id)
                    WHERE type = :type and value != 'und' and value != 'unknown' """

    extra = """ GROUP BY dd.id ORDER BY count DESC """

    if limit:
        params['limit'] = limit
        extra += " LIMIT :limit "

    if doi:
        query += 'AND publication_doi=:doi '
        params['doi'] = doi

    query += extra
    s = text(query)
    # print(query)
    # print(params)

    if 'doi' in params and 'limit' in params:
        # print('type, doi, limit')
        s = s.bindparams(bindparam('type'), bindparam('doi'), bindparam('limit'))
    elif 'doi' in params:
        # print('doi, type')
        s = s.bindparams(bindparam('type'), bindparam('doi'))
    elif 'limit' in params:
        # print('type, limit')
        s = s.bindparams(bindparam('type'), bindparam('limit'))
    else:
        # print('type')
        s = s.bindparams(bindparam('type'))

    return session.execute(s, params).fetchall()


trending_time_definition = {
    'currently': {
        'name': 'currently',
        'trending_bucket': 'trending_currently',
        'duration': timedelta(hours=-6),
        'window_size': timedelta(minutes=6),
    },
    'today': {
        'name': 'today',
        'trending_bucket': 'trending_today',
        'duration': timedelta(hours=-24),
        'window_size': timedelta(minutes=24),
    },
    'week': {
        'name': 'week',
        'trending_bucket': 'trending_week',
        'duration': timedelta(days=-7),
        'window_size': timedelta(minutes=168),
    },
    'month': {
        'name': 'month',
        'trending_bucket': 'trending_month',
        'duration': timedelta(days=-30),
        'window_size': timedelta(minutes=720),
    },
    'year': {
        'name': 'year',
        'trending_bucket': 'trending_year',
        'duration': timedelta(days=-365),
        'window_size': timedelta(minutes=8760),
    },
}


def get_numbers_influx(query_api, dois, duration="currently", fields=None):
    if fields is None:
        fields = ["score"]

    query = """
            _start = _duration_time
            _stop =  now()
            
            """

    params = {
        '_duration_time': trending_time_definition[duration]['duration'],
        '_bucket': trending_time_definition[duration]['name'],
    }

    filter_obj = None
    if dois:
        filter_obj = doi_filter_list(dois, params)

    for field in fields:
        query += get_number_influx(filter_obj, duration, field)
    # print(query)
    a = time.time()
    if dois:
        # print(filter_obj['params'])
        tables = query_api.query(query, params=filter_obj['params'])
    else:
        # print(params)
        tables = query_api.query(query, params=params)

    # print(time.time() - a)
    result = {}
    for table in tables:
        for record in table.records:
            result[record['result']] = record['_value']
    return result


def get_number_influx(filter_obj, duration="currently", field="score"):
    aggregation_field = {
        'bot_rating': 'mean',
        'contains_abstract_raw': 'mean',
        'exclamations': 'mean',
        'followers': 'sum',
        'length': 'mean',
        'questions': 'mean',
        'score': 'sum',
        'sentiment_raw': 'mean',
        "pub_count": "count",
        "count": "count"
    }

    if field not in aggregation_field:
        # print('not in field')
        # return PlainTextResponse(content='field ' + field + ' not found')
        return None

    else:
        aggregator = aggregation_field[field]

        field_selector = field
        if field == "count":
            field = "temp_count"  # avoid trouble with renaming a function (count())
            if duration == "currently":
                field_selector = "score"
            else:
                aggregator = "sum"

        if field == "pub_count":
            field_selector = 'score'

        query = field + ''' = from(bucket: _bucket)
                |> range(start: _start, stop: _stop)
                |> filter(fn: (r) => r["_measurement"] == "trending")
                |> filter(fn: (r) => r["_field"] == "''' + field_selector + '")'
        if filter_obj:
            query += filter_obj['string']
        query += """
                 |> group()
                 """
        if field == "pub_count":
            query += '|> distinct(column: "doi")'

        if field == "temp_count":
            field = "count"

        query += '''
                 |> ''' + aggregator + '''()
                 |> keep(columns: ["_value", "_time", "doi"])
                 |> yield(name: "''' + field + '''")
                 
            '''
        return query


# extend this by newest trending data?
def get_profile_information_for_doi(query_api, dois, duration="currently"):
    params = {
        "_start": trending_time_definition[duration]['duration'],
        "_bucket": trending_time_definition[duration]['name'],
    }
    filter_obj = None
    if dois:
        filter_obj = doi_filter_list(dois, params)
    query = """
        import "math"

        _stop = now()
        table = from(bucket: _bucket)
          |> range(start: _start, stop: _stop)
          |> filter(fn: (r) => r["_measurement"] == "trending") """
    if filter_obj:
        query += filter_obj['string']
    query += """
        score = table
          |> filter(fn: (r) => r["_field"] == "score")
          |> median()   
          |> keep(columns: ["_value", "doi"])
          |> rename(columns: {_value: "median_score"})
    
        sentiment = table
          |> filter(fn: (r) => r["_field"] == "sentiment_raw")
          |> mean()   
          |> keep(columns: ["_value", "doi"])
          |> rename(columns: {_value: "median_sentiment"})
    
        j1 = join(
            tables: {score:score, sentiment:sentiment},
            on: ["doi"]
            )
    
        follower = table
          |> filter(fn: (r) => r["_field"] == "followers")
          |> sum()   
          |> keep(columns: ["_value", "doi"])
          |> rename(columns: {_value: "sum_followers"})
    
        j2 = join(
            tables: {j1:j1, follower:follower},
            on: ["doi"]
            )
    
        abstract = table
          |> filter(fn: (r) => r["_field"] == "contains_abstract_raw")
          |> median()   
          |> keep(columns: ["_value", "doi"])
          |> rename(columns: {_value: "median_abstract"})
    
        j3 = join(
            tables: {j2:j2, abstract:abstract},
            on: ["doi"]
            )
    
        exclamations = table
          |> filter(fn: (r) => r["_field"] == "exclamations")
          |> mean()   
          |> keep(columns: ["_value", "doi"])
          |> rename(columns: {_value: "mean_exclamations"})
    
        j4 = join(
            tables: {j3:j3, exclamations:exclamations},
            on: ["doi"]
            )
    
        questions = table
          |> filter(fn: (r) => r["_field"] == "questions")
          |> mean()   
          |> keep(columns: ["_value", "doi"])
          |> rename(columns: {_value: "mean_questions"})
    
        j5 = join(
            tables: {j4:j4, questions:questions},
            on: ["doi"]
            )
    
        length = table
          |> filter(fn: (r) => r["_field"] == "length")
          |> mean()   
          |> keep(columns: ["_value", "doi"])
          |> rename(columns: {_value: "mean_length"})
    
        j6 = join(
            tables: {j5:j5, length:length},
            on: ["doi"]
            )

        bot = table
          |> filter(fn: (r) => r["_field"] == "bot_rating")
          |> mean()   
          |> keep(columns: ["_value", "doi"])
          |> rename(columns: {_value: "mean_bot_rating"})
        
        j7 = join(
            tables: {j6:j6, bot:bot},
            on: ["doi"]
            )
          |> yield()
        """

    # print(query)

    if dois:
        # print(filter_obj['params'])
        tables = query_api.query(query, params=filter_obj['params'])
    else:
        # print(params)
        tables = query_api.query(query, params=params)

    for table in tables:
        for record in table.records:
            result = {
                'publication': {
                    'doi': record['doi'],
                    'mean_exclamations': record['mean_exclamations'],
                    'mean_length': record['mean_length'],
                    'mean_questions': record['mean_questions'],
                    'median_abstract': record['median_abstract'],
                    'median_score': record['median_score'],
                    'median_sentiment': record['median_sentiment'],
                    'sum_followers': record['sum_followers'],
                    'mean_bot_rating': record['mean_bot_rating'],
                }
            }
            return result


def fetch_with_doi_filter(session: Session, query, doi):
    if doi:
        query += 'WHERE publication_doi=:doi'
        params = {'doi': doi, }
        s = text(query)
        s = s.bindparams(bindparam('doi'))
        return session.execute(s, params).fetchall()
    s = text(query)
    return session.execute(s).fetchall()


def get_top_n_dois(query_api, duration="currently", field="count", n=5):
    if field == "count":
        params = {
            '_window_time': trending_time_definition[duration]['window_size'],
            "_duration_time": timedelta(seconds=-trending_time_definition[duration]['duration'].total_seconds()),
            '_bucket': trending_time_definition[duration]['name'],
            '_n': n,
        }
        query = """
            import "experimental"
            import "date"
            
            _start = experimental.subDuration(d: _duration_time, from: date.truncate(t: now(), unit: _window_time))
            _stop =  date.truncate(t: now(), unit: _window_time)
            
            a = from(bucket: _bucket)
                |> range(start: _start, stop:  _stop)
                |> filter(fn: (r) => r["_measurement"] == "trending")
                |> filter(fn: (r) => r["_field"] == "score")
                |> count()
                |> group()
                |> sort(desc: true)
                |> keep(columns: ["_value", "doi"])
                |> rename(columns: {_value: "count"})   
                |> limit(n: _n)
                |> yield()
        """
        # print(query)
        # print(params)

        tables = query_api.query(query, params=params)
        results = []
        for table in tables:
            for record in table.records:
                results.append(record['doi'])
        # print(results)
        return results


def get_top_n_trending_dois(query_api, duration="currently", field="count", n=5):
    if field == "count":
        params = {
            '_window_time': trending_time_definition[duration]['window_size'],
            '_duration_time': timedelta(seconds=-trending_time_definition[duration]['duration'].total_seconds()),
            '_bucket': trending_time_definition[duration]['trending_bucket'],

            '_n': n,
        }
        query = """
            import "experimental"
            import "date"

            _start = experimental.subDuration(d: _duration_time, from: date.truncate(t: now(), unit: _window_time))
            _stop =  date.truncate(t: now(), unit: _window_time)

            a = from(bucket: _bucket)
                |> range(start: _start, stop:  _stop)
                |> filter(fn: (r) => r["_measurement"] == "trending")
                |> filter(fn: (r) => r["_field"] == "score")
                |> sum()
                |> group()
                |> sort(desc: true)
                |> keep(columns: ["_value", "doi"])
                |> rename(columns: {_value: "count"})   
                |> limit(n: _n)
                |> yield()
        """

        tables = query_api.query(query, params=params)
        results = []
        for table in tables:
            for record in table.records:
                results.append(record['doi'])
        return results


def get_window_chart_data(query_api, duration="currently", field="score", n=5, dois=None):
    aggregation_field = {
        'bot_rating': 'mean',
        'contains_abstract_raw': 'median',
        'exclamations': 'mean',
        'followers': 'sum',
        'length': 'mean',
        'questions': 'mean',
        'score': 'sum',
        'sentiment_raw': 'median',
        "count": "count"
    }

    if field not in aggregation_field:
        print('error')  # todo
        return None

    else:
        aggregator = aggregation_field[field]

        if field == "count":
            if duration == "currently":
                field = "score"
            else:
                aggregator = "sum"

        params = {
            '_field_name': field,
            '_window_time': trending_time_definition[duration]['window_size'],
            "_duration_time": timedelta(seconds=-trending_time_definition[duration]['duration'].total_seconds()),
            '_bucket': trending_time_definition[duration]['name'],
        }

        # print(dois)
        doi_list = dois
        if not dois or len(dois) == 0 or dois is None:
            doi_list = get_top_n_trending_dois(query_api, duration, "count", n)
        if not dois or len(dois) == 0 or dois is None:
            doi_list = get_top_n_dois(query_api, duration, "count", n)
        if len(doi_list) > n:
            doi_list = doi_list[0:n]
        filter_obj = doi_filter_list(doi_list, params)
        query = """
            import "experimental"
            import "date"
            
            _start = experimental.subDuration(d: _duration_time, from: date.truncate(t: now(), unit: _window_time))
            _stop =  date.truncate(t: now(), unit: _window_time)
    
           a = from(bucket: _bucket)
             |> range(start: _start, stop:  _stop)
             |> filter(fn: (r) => r["_measurement"] == "trending")
             |> filter(fn: (r) => r["_field"] == _field_name)"""
        query += filter_obj['string']
        query += """
             |> aggregateWindow(every: _window_time, fn: """ + aggregator + """, createEmpty: false)
             |> keep(columns: ["_value", "_time", "_field", "doi"])
             |> yield()
        """
        # print(query)
        print(filter_obj['params'])

        a = time.time()
        tables = query_api.query(query, params=filter_obj['params'])
        # print(time.time() - a)

        results = []
        for table in tables:

            doi = None
            data = []
            for record in table.records:
                point = {'time': record['_time'].strftime("%Y-%m-%dT%H:%M:%SZ"), 'value': record['_value']}
                data.append(point)
                if not doi:
                    doi = record['doi']
            results.append({
                "doi": doi,
                "data": data,
            })

        return results


def get_trending_chart_data(query_api, duration="currently", field="score", n=5, dois=None):
    fields = ['score', 'count', 'median_sentiment', 'sum_follower', 'abstract_difference', 'median_age',
              'median_length', 'mean_questions', 'mean_exclamations', 'mean_bot_rating', 'projected_change',
              'trending', 'ema', 'kama', 'ker', 'mean_score', 'stddev']

    if field not in fields:
        # print('error')  # todo
        return None

    params = {
        '_field_name': field,
        '_window_time': trending_time_definition[duration]['window_size'],
        "_duration_time": timedelta(seconds=-trending_time_definition[duration]['duration'].total_seconds()),
        '_bucket': trending_time_definition[duration]['trending_bucket'],
    }

    doi_list = dois
    if not dois:
        doi_list = get_top_n_trending_dois(query_api, duration, "count", n)

    if len(doi_list) > n:
        doi_list = doi_list[0:n]

    filter_obj = doi_filter_list(doi_list, params)

    query = """
        import "experimental"
        import "date"
        
        _start = experimental.subDuration(d: _duration_time, from: date.truncate(t: now(), unit: _window_time))
        _stop =  date.truncate(t: now(), unit: _window_time)

       a = from(bucket: _bucket)
         |> range(start: _start, stop:  _stop)
         |> filter(fn: (r) => r["_measurement"] == "trending") 
         |> filter(fn: (r) => r["_field"] == _field_name) """
    query += filter_obj['string']
    query += """
         |> keep(columns: ["_value", "_time", "_field", "doi"])
         |> yield()
    """

    tables = query_api.query(query, params=filter_obj['params'])
    results = []
    for table in tables:

        doi = None
        data = []
        for record in table.records:
            point = {'time': record['_time'].strftime("%Y-%m-%dT%H:%M:%SZ"), 'value': record['_value']}
            data.append(point)
            if not doi:
                doi = record['doi']
        results.append({
            "doi": doi,
            "data": data,
        })

    return results


def doi_filter_list(doi_list, params):
    if doi_list:
        filter_string = """
                |> filter(fn: (r) => """
        i = 0
        for doi in doi_list:
            filter_string += 'r["doi"] == _doi_nr_' + str(i) + ' or '
            params['_doi_nr_' + str(i)] = doi
            i += 1
        filter_string = filter_string[:-4] + ')'
        return {"string": filter_string, "params": params}
    return {"string": '', "params": params}


def system_running_check(query_api):
    params = {
        '_start': timedelta(minutes=-5),
        '_bucket': trending_time_definition['currently']['trending_bucket'],
    }

    query = """
        from(bucket: _bucket)
          |> range(start: _start)
          |> filter(fn: (r) => r["_measurement"] == "trending")
          |> filter(fn: (r) => r["_field"] == "score")
          |> group()
          |> count()
          |> yield(name: "count")
    """
    # print(query)
    # print(params)
    tables = query_api.query(query, params=params)
    count = 0
    for table in tables:
        for record in table.records:
            count = record['_value']

    if count > 10:
        return 'ok'

    return 'not running'
