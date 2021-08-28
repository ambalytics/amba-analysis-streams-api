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
                'words': '$subj.processed.words',
                'contains_abstract': '$subj.processed.contains_abstract',
                'bot_rating': '$subj.processed.bot_rating',
                'question_mark_count': '$subj.processed.question_mark_count',
                'exclamation_mark_count': '$subj.processed.exclamation_mark_count',
                'hashtags': '$subj.processed.hashtags',
                'annotations': '$subj.processed.annotations',
                'a_types': '$subj.processed.a_types',
                'length': '$subj.processed.length'
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
                'words': {
                    '$push': '$words'
                },
                'hashtags': {
                    '$addToSet': '$hashtags'
                },
                'annotations': {
                    '$addToSet': '$annotations'
                },
                'a_types': {
                    '$addToSet': '$a_types'
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
                'words': {
                    '$reduce': {
                        'input': '$words',
                        'initialValue': [],
                        'in': {
                            '$concatArrays': [
                                '$$value', '$$this'
                            ]
                        }
                    }
                },
                'hashtags': {
                    '$reduce': {
                        'input': '$hashtags',
                        'initialValue': [],
                        'in': {
                            '$concatArrays': [
                                '$$value', '$$this'
                            ]
                        }
                    }
                },
                'annotations': {
                    '$reduce': {
                        'input': '$annotations',
                        'initialValue': [],
                        'in': {
                            '$concatArrays': [
                                '$$value', '$$this'
                            ]
                        }
                    }
                },
                'a_types': {
                    '$reduce': {
                        'input': '$a_types',
                        'initialValue': [],
                        'in': {
                            '$concatArrays': [
                                '$$value', '$$this'
                            ]
                        }
                    }
                }
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
