import logging
from typing import Optional

import motor.motor_asyncio
from bson.objectid import ObjectId

# todo own client for publciations
config = {
    'mongo_url': "mongodb://root:example@mongo_db:27017/",
    'mongo_client': "events",
    'mongo_collection': "publication",
    'mongo_collection_events': "processed",
}

MONGO_DETAILS = config['mongo_url']
motor_client = motor.motor_asyncio.AsyncIOMotorClient(
    MONGO_DETAILS)  # https://medium.com/@bruno.fosados/simple-learn-docker-fastapi-and-vue-js-second-part-backend-fastapi-cec2e1e093a9 instead link
database = motor_client[config['mongo_client']]
publication_collection = database[config['mongo_collection']]
events_collection = database[config['mongo_collection_events']]


# todo
# make stuff easier
# add optional stuff like count, sort etc as easy functions ? exists?
# sort("count", -1)
# count


def publication_helper(publication) -> dict:
    return publication


# helper
def fix_grouped_publication(publication):
    if len(publication['_id']) > 1:
        for key, value in publication['_id'].items():
            if isinstance(value, list):
                publication[key] = value[0]
            else:
                publication[key] = value
    return publication


# Retrieve all publications present in the database
async def retrieve_publications():
    publications = []
    async for publication in publication_collection.find():
        publications.append(publication_helper(publication))
    return publications


# get number of different tweet authors
async def get_publication_count():
    result = []
    query = []
    query.extend([
        {
            '$count': 'count'
        }
    ])

    async for r in publication_collection.aggregate(query):
        result.append(publication_helper(r))
    return result


# Retrieve a publication with a matching ID
async def retrieve_publication(id):
    logging.warning('retrieve publication ' + id)
    publication = await publication_collection.find_one({"_id": str(id)})
    if publication:
        return publication_helper(publication)


# Retrieve the top publications by tweet score
async def top_publications(limit=10):
    publications = []
    async for publication in events_collection.aggregate([
        {
            '$project': {
                'doi': '$obj.data.doi',
                'score': '$subj.processed.score',
                'contains_abstract': '$subj.processed.contains_abstract',
                'bot_rating': '$subj.processed.bot_rating',
                'question_mark_count': '$subj.processed.question_mark_count',
                'exclamation_mark_count': '$subj.processed.exclamation_mark_count',
                'hashtags': {
                    '$cond': {
                        'if': {
                            '$isArray': '$subj.processed.hashtags'
                        },
                        'then': {
                            '$size': '$subj.processed.hashtags'
                        },
                        'else': '0'
                    }
                },
                'length': '$subj.processed.length',
                'followers': '$subj.processed.followers'
            }
        }, {
            '$group': {
                '_id': '$doi',
                'count': {
                    '$sum': 1
                },
                'score': {
                    '$sum': '$score'
                },
                'question_mark_count': {
                    '$sum': '$question_mark_count'
                },
                'exclamation_mark_count': {
                    '$sum': '$exclamation_mark_count'
                },
                'length': {
                    '$sum': '$length'
                },
                'contains_abstract': {
                    '$sum': '$contains_abstract'
                },
                'bot_rating': {
                    '$sum': '$bot_rating'
                },
                'hashtags': {
                    '$sum': '$hashtags'
                },
                'followers': {
                    '$sum': '$followers'
                  }
            }
        }, {
            '$project': {
                'doi': '$_id',
                'count': 1,
                'question_mark_count': 1,
                'exclamation_mark_count': 1,
                'length_avg': {
                    '$divide': [
                        '$length', '$count'
                    ]
                },
                'score': 1,
                'contains_abstract_avg': {
                    '$divide': [
                        '$contains_abstract', '$count'
                    ]
                },
                'bot_rating_avg': {
                    '$divide': [
                        '$bot_rating', '$count'
                    ]
                },
                'followers': 1,
                'hashtags': 1,
            }
        }, {
            '$sort': {
                'score': -1
            }
        }, {
            '$limit': int(limit)
        }, {
            '$lookup': {
                'from': 'publication',
                'localField': '_id',
                'foreignField': '_id',
                'as': 'publication'
            }
        }, {
            '$unwind': {
                'path': '$publication',
                'preserveNullAndEmptyArrays': False
            }
        }, {
            '$replaceRoot': {
                'newRoot': {
                    '$mergeObjects': [
                        '$publication', '$$ROOT'
                    ]
                }
            }
        }, {
            '$unset': 'publication'
        }
    ]):
        publications.append(publication_helper(publication))
    return publications


# Retrieve the top publications by tweet score
async def twitter_data_publication(doi):
    publications = []
    async for publication in events_collection.aggregate([
        {
            '$match': {
                'obj.data.doi': str(doi)
            }
        }, {
            '$replaceRoot': {
                'newRoot': {
                    '$mergeObjects': [
                        '$subj.processed', '$subj.data'
                    ]
                }
            }
        }
    ]):
        publications.append(publication_helper(publication))
    return publications
