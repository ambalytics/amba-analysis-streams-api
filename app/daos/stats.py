from sqlalchemy import text, bindparam
from sqlalchemy.orm import Session  # type: ignore


def get_types(session: Session, doi):
    query = """SELECT "type" as "value", COUNT(*) as count  
                 FROM "DiscussionData" d  
             """

    if doi:
        query += 'WHERE "publicationDoi"=:doi '
        query += 'GROUP BY "type" ORDER BY count DESC LIMIT 10 '
        params = {'doi': doi, }
        s = text(query)
        s = s.bindparams(bindparam('doi'))
        return session.execute(s, params).fetchall()
    s = text(query + 'GROUP BY "type" ORDER BY count DESC LIMIT 10')
    return session.execute(s).fetchall()


def get_sources(session: Session, doi):
    query = """WITH result AS  
                  (  
                     (  
                         SELECT "source", COUNT(*) as c, 
                         ROUND(COUNT(*) / CAST( SUM(count(*)) OVER () AS FLOAT) * 100) as p  
                         FROM "DiscussionData" d   """
    extra = """    GROUP BY "source"
                         ORDER BY c DESC  
                         LIMIT 10  
                      )			  
                      UNION  
                      (  
                         SELECT 'total' as language, COUNT(*) as c, 100 as p  
                         FROM "DiscussionData" d  
                       )	  
                   )  
                SELECT "source" as value, c as count FROM result WHERE result.p > 1"""

    if doi:
        query += ' WHERE "publicationDoi"=:doi'
        query += extra
        params = {'doi': doi, }
        s = text(query)
        s = s.bindparams(bindparam('doi'))
        return session.execute(s, params).fetchall()
    s = text(query + extra)
    return session.execute(s).fetchall()


def get_words(session: Session, doi):
    # add percentage or something to allow world word cloud
    query = """ SELECT SUM(dwd.count) as count, dw.word as value FROM "DiscussionData" as d
                    JOIN "DiscussionWordData" as dwd ON (dwd."discussionDataId" = d."id")
                    JOIN "DiscussionWord" as dw ON (dwd."discussionWordId" = dw."id")"""

    if doi:
        query += 'WHERE "publicationDoi"=:doi '
        query += 'GROUP BY dw.id'
        params = {'doi': doi, }
        s = text(query)
        s = s.bindparams(bindparam('doi'))
        return session.execute(s, params).fetchall()
    s = text(query + 'GROUP BY dw.id')
    return session.execute(s).fetchall()


def get_top_lang(session: Session, doi):
    # todo limit and treshold
    query = """WITH result AS  
              (  
                 (  
                     SELECT "language", COUNT(*) as c, 
                     ROUND(COUNT(*) / CAST( SUM(count(*)) OVER () AS FLOAT) * 100) as p  
                     FROM "DiscussionData" d   """
    extra = """   GROUP BY "language"
                     ORDER BY c DESC  
                     LIMIT 10  
                  )			  
                  UNION  
                  (  
                     SELECT 'total' as language, COUNT(*) as c, 100 as p  
                     FROM "DiscussionData" d  
                   )	  
               )  
                SELECT "language" as value, c as count FROM result WHERE result.p > 1"""

    if doi:
        query += ' WHERE "publicationDoi"=:doi'
        query += extra
        params = {'doi': doi, }
        s = text(query)
        s = s.bindparams(bindparam('doi'))
        return session.execute(s, params).fetchall()
    s = text(query + extra)
    return session.execute(s).fetchall()


def get_top_entities(session: Session, doi):
    query = """WITH result AS  
              (  
                 (  
                    SELECT "entity", COUNT(*) as count, 
                     ROUND(COUNT(*) / CAST( SUM(count(*)) OVER () AS FLOAT) * 100) as p  
                    FROM "DiscussionData" d  
                    JOIN "DiscussionEntityData" as dwd ON (dwd."discussionDataId" = d."id")
                    JOIN "DiscussionEntity" as dw ON (dwd."discussionEntityId" = dw."id")
                    """
    extra = """  GROUP BY "entity"
                    ORDER BY count DESC  
                    LIMIT 10)			  
                  UNION  
                  (  
                     SELECT 'total' as entity, COUNT(*) as c, 100 as p  
                     FROM "DiscussionEntityData" d  
                   )	  
               )  
                SELECT "entity" as value, count FROM result WHERE result.p > 1"""

    if doi:
        query += ' WHERE "publicationDoi"=:doi'
        query += extra
        params = {'doi': doi, }
        s = text(query)
        s = s.bindparams(bindparam('doi'))
        return session.execute(s, params).fetchall()
    s = text(query + extra)
    return session.execute(s).fetchall()


def get_top_hashtags(session: Session, doi):
    query = """SELECT "hashtag" as value, COUNT(*) as count
                FROM "DiscussionData" d  
                JOIN "DiscussionHashtagData" as dwd ON (dwd."discussionDataId" = d."id")
                JOIN "DiscussionHashtag" as dw ON (dwd."discussionHashtagId" = dw."id") """
    extra = """ GROUP BY "hashtag"
                ORDER BY count DESC  
                LIMIT 10 """
    if doi:
        query += ' WHERE d."publicationDoi"=:doi '
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


# get list of user countries
def get_country_list(session: Session, doi):
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
    return fetchWithDoiFilter(session, query, doi)


# get number of different tweet authors
def get_tweet_count(session: Session, doi):
    query = 'SELECT COUNT (*) FROM "DiscussionData"'
    return fetchWithDoiFilter(session, query, doi)


# get total sum of followers reached
def get_followers_reached(session: Session, doi):
    query = 'SELECT SUM(followers) FROM "DiscussionData" '
    return fetchWithDoiFilter(session, query, doi)


# get average score
def get_total_score(session: Session, doi):
    query = 'SELECT SUM(score) FROM "DiscussionData"'
    return fetchWithDoiFilter(session, query, doi)


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


def fetchWithDoiFilter(session: Session, query, doi):
    if doi:
        query += 'WHERE "publicationDoi"=:doi'
        params = {'doi': doi, }
        s = text(query)
        s = s.bindparams(bindparam('doi'))
        return session.execute(s, params).fetchall()
    s = text(query)
    return session.execute(s).fetchall()
