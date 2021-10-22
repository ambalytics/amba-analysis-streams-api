import time
from datetime import timedelta
from sqlalchemy import text, bindparam
from sqlalchemy.orm import Session  # type: ignore

# time definitions used by influxdb
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


def get_tweet_author_count(doi, session: Session, id, mode="publication"):
    """ get tweet author count from postgresql """
    params = {}
    if mode == "fieldOfStudy":
        query = """SELECT count(id) as count 
                    FROM discussion_data_point as ddp
                         JOIN discussion_data as dd ON (ddp.discussion_data_point_id = dd.id)
                         JOIN publication_field_of_study as pfos on ddp.publication_doi = pfos.publication_doi
                    WHERE type = 'name' AND pfos.field_of_study_id=:id """
        params['id'] = id
    elif mode == "author":
        query = """SELECT count(id) as count 
                    FROM discussion_data_point as ddp
                         JOIN discussion_data as dd ON (ddp.discussion_data_point_id = dd.id)
                         JOIN publication_author as pfos on ddp.publication_doi = pfos.publication_doi
                    WHERE type = 'name'  AND pfos.author_id=:id """
        params['id'] = id
    else:  # default publication
        query = """SELECT count(id) as count
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
    """ get newest tweet from postgresql """
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
    """ get dois for a given field of study from postgresql """
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
    """ get dois for a given author from postgresql """
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
    """ get profile information avg, min, max from postgresql """
    query = """
        SELECT 'mean_score' as type, MIN(mean_score), MAX(mean_score), AVG(mean_score)
        FROM trending
        WHERE duration = :duration
        UNION
        SELECT 'mean_bot_rating' as type, MIN(mean_bot_rating), MAX(mean_bot_rating), AVG(mean_bot_rating)
        FROM trending
        WHERE duration = :duration
        UNION
        SELECT 'mean_sentiment' as type, MIN(mean_sentiment), MAX(mean_sentiment), AVG(mean_sentiment)
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
        SELECT 'mean_length' as type, MIN(mean_length), MAX(mean_length), AVG(mean_length)
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
    """ get discussion types with count an percentage from postgresql """
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
    """ get discussion types with count from postgresql """
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


def get_numbers_influx(query_api, dois, duration="currently", fields=None):
    """ get numbers from influx, switch between getting (total) and calculating (for dois) """
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

    if dois:
        filter_obj = doi_filter_list(dois, params)

        # print('get numbers')
        for field in fields:
            query += get_number_influx(filter_obj, duration, field)

        tables = query_api.query(query, params=filter_obj['params'])
    else:
        # print('get task numbers')
        for field in fields:
            query += get_task_number_influx(duration, field)

        # print(query)
        tables = query_api.query(query, params=params)

    result = {}
    for table in tables:
        for record in table.records:
            result[record['result']] = record['_value']
    return result


def get_task_number_influx(duration="currently", field="score"):
    """ get number from task (own bucket for total numbers, returns string query) """
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
        query = field + ''' = from(bucket: "numbers")
                |> range(start: _start, stop: _stop)
                |> filter(fn: (r) => r["_measurement"] == "''' + duration + '''")
                |> filter(fn: (r) => r["_field"] == "''' + field + '''")
                |> last()
                |> keep(columns: ["_value", "_time", "doi"])
                |> yield(name: "''' + field + '''")
                
            '''
        return query


def get_number_influx(filter_obj, duration="currently", field="score"):
    """ get influx number for specific dois calculating, returns string query"""
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


def get_profile_information_for_doi(session: Session, doi, id, mode="publication", duration="currently"):
    """ get profile information for a doi, fieldOfStudy or Author"""
    query = """
            SELECT AVG(mean_score)          mean_score,
                   AVG(abstract_difference) abstract_difference,
                   AVG(mean_sentiment)      mean_sentiment,
        """
    if mode == "publication":
        query += "SUM(sum_followers) sum_followers,"
    else:
        query += "AVG(sum_followers) sum_followers,"
    query += """
            AVG(mean_length) mean_length,
            AVG(mean_questions) mean_questions,
            AVG(mean_exclamations) mean_exclamations,
            AVG(mean_bot_rating) mean_bot_rating
            FROM (
                     SELECT DISTINCT doi,
                                     mean_score,
                                     abstract_difference,
                                     mean_sentiment,
                                     sum_followers,
                                     mean_length,
                                     mean_questions,
                                     mean_exclamations,
                                     mean_bot_rating
                     FROM trending t
                              JOIN publication p on p.doi = t.publication_doi
                              JOIN publication_field_of_study pfos on p.doi = pfos.publication_doi
                              JOIN publication_author pa on p.doi = pa.publication_doi
        WHERE duration = :duration
    """

    params = {'duration': duration}
    if mode == "publication":
        query += "AND doi = :doi"
        params['doi'] = doi
    if mode == "author":
        query += "AND author_id = :id"
        params['id'] = id
    if mode == "fieldOfStudy":
        query += "AND field_of_study_id = :id"
        params['id'] = id
    query += "  ) t"

    s = text(query)
    # print(query)
    # print(params)

    if 'id' in params:
        s = s.bindparams(bindparam('duration'), bindparam('id'))
    elif 'doi' in params:
        s = s.bindparams(bindparam('duration'), bindparam('doi'))

    return session.execute(s, params).fetchone()._asdict()


# todo check remove
def fetch_with_doi_filter(session: Session, query, doi):
    """ postresql helper to add a doi filter """
    if doi:
        query += 'WHERE publication_doi=:doi'
        params = {'doi': doi, }
        s = text(query)
        s = s.bindparams(bindparam('doi'))
        return session.execute(s, params).fetchall()
    s = text(query)
    return session.execute(s).fetchall()


def get_top_n_dois(query_api, duration="currently", field="count", n=5):
    """ get top n dois by count, expensive query, influx db """
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


def get_top_n_trending_dois(session: Session, duration="currently", n=5):
    """ get top trending dois, postgresql """
    query = """
            SELECT publication_doi FROM trending t WHERE duration = :duration ORDER BY score DESC LIMIT :n;
            """

    params = {'duration': duration, 'n': n}
    s = text(query)
    s = s.bindparams(bindparam('duration'), bindparam('n'))
    rows = session.execute(s, params).fetchall()

    result = []
    for r in rows:
        result.append(r[0])

    # print(result)
    return result


def get_window_chart_data(query_api, session: Session, duration="currently", field="score", n=5, dois=None):
    """ get window value data for given dois over a period of time from influx, mainly used for charts """
    aggregation_field = {
        'bot_rating': 'mean',
        'contains_abstract_raw': 'mean',
        'exclamations': 'mean',
        'followers': 'sum',
        'length': 'mean',
        'questions': 'mean',
        'score': 'sum',
        'sentiment_raw': 'mean',
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
        if not doi_list or len(doi_list) == 0 or doi_list is None:
            doi_list = get_top_n_trending_dois(session, duration, n)
        if not doi_list or len(doi_list) == 0 or doi_list is None:
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
        # print(filter_obj['params'])

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


def get_trending_chart_data(query_api, session: Session, duration="currently", field="score", n=5, dois=None):
    """ get trending data for given dois over a period of time from influx, mainly used for charts """
    fields = ['score', 'count', 'mean_sentiment', 'sum_followers', 'abstract_difference', 'mean_age',
              'mean_length', 'mean_questions', 'mean_exclamations', 'mean_bot_rating', 'projected_change',
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
    if not doi_list:
        doi_list = get_top_n_trending_dois(session, duration, n)

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
    """ influx helper adding a doi filter list (faster than array check in influx) """
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
    """ check if the system is running correctly by counting how many tweets we got in the last 5 minutes,
        if over 10 than ok"""
    query = """
        from(bucket: "currently)
          |> range(start: -5m)
          |> filter(fn: (r) => r["_measurement"] == "trending")
          |> filter(fn: (r) => r["_field"] == "score")
          |> group()
          |> count()
          |> yield(name: "count")
    """
    # print(query)
    # print(params)
    tables = query_api.query(query)
    count = 0
    for table in tables:
        for record in table.records:
            count = record['_value']

    if count > 10:
        return 'ok'

    return 'not running'
