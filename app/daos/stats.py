from sqlalchemy import text, bindparam
from sqlalchemy.orm import Session  # type: ignore


def get_types(session: Session, doi, **kwargs):
    query = """SELECT "type" as "value", COUNT(*) as count  
                 FROM "DiscussionData" d  
             """

    if doi:
        query += 'WHERE "publicationDoi"=:doi '
        query += 'GROUP BY "type" ORDER BY count DESC '
        params = {'doi': doi, }
        s = text(query)
        s = s.bindparams(bindparam('doi'))
        return session.execute(s, params).fetchall()
    s = text(query + 'GROUP BY "type" ORDER BY count DESC ')
    return session.execute(s).fetchall()


def get_sentiment(session: Session, doi, **kwargs):
    query = """SELECT CASE 
                            WHEN sentiment=1 THEN 'negative'
                            WHEN sentiment=10 THEN 'positive'
                            ELSE 'neutral'
                END as value, COUNT(*) as count  
                FROM "DiscussionData" d  
             """

    if doi:
        query += 'WHERE "publicationDoi"=:doi '
        query += 'GROUP BY value ORDER BY value DESC '
        params = {'doi': doi, }
        s = text(query)
        s = s.bindparams(bindparam('doi'))
        return session.execute(s, params).fetchall()
    s = text(query + 'GROUP BY value ORDER BY value DESC ')
    return session.execute(s).fetchall()


def get_sources(session: Session, doi, limit: int = 10, min_percentage: float = 1):
    query = """WITH result AS  
              (  
                 (  
                     SELECT "source", COUNT(*) as c, 
                     ROUND(COUNT(*) / CAST( SUM(count(*)) OVER () AS FLOAT) * 100) as p  
                     FROM "DiscussionData" d   """
    extra1 = """   GROUP BY "source"
                     ORDER BY c DESC  
                     LIMIT :limit
                  )			  
                  UNION  
                  (  
                     SELECT 'total' as source, COUNT(*) as c, 100 as p  
                     FROM "DiscussionData" d  """
    extra2 = """ )	  
               )  
                SELECT "source" as value, c as count FROM result WHERE result.p >= :mp ORDER BY count DESC """


    if doi:
        doiSelector = ' WHERE "publicationDoi"=:doi'
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


def get_words(session: Session, doi, limit: int = 100, **kwargs):
    # add percentage or something to allow world word cloud
    query = """ SELECT SUM(dwd.count) as count, dw.word as value FROM "DiscussionData" as d
                    JOIN "DiscussionWordData" as dwd ON (dwd."discussionDataId" = d."id")
                    JOIN "DiscussionWord" as dw ON (dwd."discussionWordId" = dw."id") """
    extra = """ GROUP BY dw.id ORDER BY count DESC LIMIT :limit """

    if doi:
        query += 'WHERE "publicationDoi"=:doi '
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


def get_top_lang(session: Session, doi, limit: int = 10, min_percentage: float = 1):
    query = """WITH result AS  
              (  
                 (  
                     SELECT "language", COUNT(*) as c, 
                     ROUND(COUNT(*) / CAST( SUM(count(*)) OVER () AS FLOAT) * 100) as p  
                     FROM "DiscussionData" d   """
    extra1 = """   GROUP BY "language"
                     ORDER BY c DESC  
                     LIMIT :limit
                  )			  
                  UNION  
                  (  
                     SELECT 'total' as language, COUNT(*) as c, 100 as p  
                     FROM "DiscussionData" d  """
    extra2 = """ )	  
               )  
                SELECT "language" as value, c as count FROM result WHERE result.p >= :mp ORDER BY count DESC """

    if doi:
        doiSelector = ' WHERE "publicationDoi"=:doi'
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


def get_top_entities(session: Session, doi, limit: int = 20, min_percentage: float = 1):
    query = """WITH result AS  
              (  
                 (  
                    SELECT "entity", COUNT(*) as count, 
                     ROUND(COUNT(*) / CAST( SUM(count(*)) OVER () AS FLOAT) * 100) as p  
                    FROM "DiscussionData" d  
                    JOIN "DiscussionEntityData" as dwd ON (dwd."discussionDataId" = d."id")
                    JOIN "DiscussionEntity" as dw ON (dwd."discussionEntityId" = dw."id")
                    """

    if doi:
        query += """WHERE "publicationDoi"=:doi
                   GROUP BY "entity"
                            ORDER BY count DESC  
                            LIMIT :limit)			  
                          UNION  
                          (  
                             SELECT 'total' as entity, COUNT(*) as c, 100 as p  
                              FROM "DiscussionData" d  
                               JOIN "DiscussionEntityData" as dwd ON (dwd."discussionDataId" = d."id")
                               JOIN "DiscussionEntity" as dw ON (dwd."discussionEntityId" = dw."id")
                              WHERE "publicationDoi"=:doi      
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
                     FROM "DiscussionEntityData" d  
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
                         SELECT "hashtag", COUNT(*) as c, 
                         ROUND(COUNT(*) / CAST( SUM(count(*)) OVER () AS FLOAT) * 100) as p  
                        FROM "DiscussionData" d  
                        JOIN "DiscussionHashtagData" as dwd ON (dwd."discussionDataId" = d."id")
                        JOIN "DiscussionHashtag" as dw ON (dwd."discussionHashtagId" = dw."id")        """
    extra1 = """   GROUP BY "hashtag"
                         ORDER BY c DESC  
                         LIMIT :limit
                      )			  
                      UNION  
                      (  
                         SELECT 'total' as hashtag, COUNT(*) as c, 100 as p  
                        FROM "DiscussionData" d  
                        JOIN "DiscussionHashtagData" as dwd ON (dwd."discussionDataId" = d."id")
                        JOIN "DiscussionHashtag" as dw ON (dwd."discussionHashtagId" = dw."id")  """
    extra2 = """ )	  
                   )  
                    SELECT "hashtag" as value, c as count FROM result WHERE result.p >= :mp ORDER BY count DESC """

    if doi:
        doiSelector = ' WHERE "publicationDoi"=:doi'
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
def get_country_list(session: Session, doi, **kwargs):
    query = """
    SELECT COUNT("authorLocation"), "authorLocation" FROM "DiscussionData" WHERE "authorLocation" != 'unknown' 
    """
    if doi:
        query += ' AND "publicationDoi"=:doi GROUP BY "authorLocation"'
        params = {'doi': doi, }
        s = text(query)
        s = s.bindparams(bindparam('doi'))
        return session.execute(s, params).fetchall()
    s = text(query + 'GROUP BY "authorLocation"')
    return session.execute(s).fetchall()


# get number of different tweet authors
def get_tweet_author_count(session: Session, doi):
    query = 'SELECT COUNT(*) FROM (SELECT DISTINCT "authorName", "publicationDoi" FROM "DiscussionData") AS temp '
    return fetch_with_doi_filter(session, query, doi)


# get number of different tweet authors
def get_tweet_count(session: Session, doi):
    query = 'SELECT COUNT (*) FROM "DiscussionData"'
    return fetch_with_doi_filter(session, query, doi)


# get total sum of followers reached
def get_followers_reached(session: Session, doi):
    query = 'SELECT SUM(followers) FROM "DiscussionData" '
    return fetch_with_doi_filter(session, query, doi)


# get average score
def get_total_score(session: Session, doi):
    query = 'SELECT SUM(score) FROM "DiscussionData"'
    return fetch_with_doi_filter(session, query, doi)


# get tweets binned every hour
def get_time_count_binned(session: Session, doi):
    query = """SELECT COUNT(*) as count, 
                     date_part('year', "createdAt") as year, 
                     date_part('month', "createdAt") as month, 
                     date_part('day', "createdAt") as day, 
                     date_part('hour', "createdAt") as hour
                FROM "DiscussionData" d  """
    extra = """ GROUP BY date_part('year', "createdAt"), date_part('month', "createdAt"), 
                   date_part('day', "createdAt"), date_part('hour', "createdAt")  
                ORDER BY year, month, day, hour;  
                """
    if doi:
        query += ' WHERE "publicationDoi"=:doi'
        query += extra
        params = {'doi': doi, }
        s = text(query)
        s = s.bindparams(bindparam('doi'))
        return session.execute(s, params).fetchall()
    s = text(query + extra)
    return session.execute(s).fetchall()


# get hour binned periodic count
def get_tweet_time_of_day(session: Session, doi):
    query = """SELECT COUNT(*) / COUNT(distinct DATE("createdAt")) as count, 
                date_part('hour', "createdAt") AS "value" FROM "DiscussionData"
                """
    if doi:
        query += ' WHERE "publicationDoi"=:doi GROUP BY "value"'
        params = {'doi': doi, }
        s = text(query)
        s = s.bindparams(bindparam('doi'))
        return session.execute(s, params).fetchall()
    s = text(query + 'GROUP BY "value"')
    return session.execute(s).fetchall()


def fetch_with_doi_filter(session: Session, query, doi):
    if doi:
        query += 'WHERE "publicationDoi"=:doi'
        params = {'doi': doi, }
        s = text(query)
        s = s.bindparams(bindparam('doi'))
        return session.execute(s, params).fetchall()
    s = text(query)
    return session.execute(s).fetchall()
