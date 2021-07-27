from typing import Optional

import motor.motor_asyncio
from bson.objectid import ObjectId

# todo own client for publciations
config = {
    'mongo_url': "mongodb://root:example@mongo_db:27017/",
    'mongo_client': "events",
    'mongo_collection': "linked",
}

MONGO_DETAILS = config['mongo_url']
motor_client = motor.motor_asyncio.AsyncIOMotorClient(
    MONGO_DETAILS)  # https://medium.com/@bruno.fosados/simple-learn-docker-fastapi-and-vue-js-second-part-backend-fastapi-cec2e1e093a9 instead link
database = motor_client[config['mongo_client']]
publication_collection = database[config['mongo_collection']]


# todo
# make stuff easier
# add optional stuff like count, sort etc as easy functions ? exists?
# sort("count", -1)
# count


def publication_helper(publication) -> dict:
    return publication


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
    publication = await publication_collection.find_one({"$obj.data.id": str(id)})
    if publication:
        return publication_helper(publication)


# Retrieve the top publications by tweet count
async def top_publications(limit=10):
    publications = []
    async for publication in publication_collection.aggregate([
        {
            '$group': {
                '_id': {
                    'doi': '$obj.data.doi',
                    'name': '$obj.data.title'
                },
                'count': {
                    '$sum': 1
                }
            }
        }, {
            '$sort': {
                'count': -1
            }
        }, {
            '$limit': int(limit)
        }
    ]):
        publications.append(fix_grouped_publication(publication))
    return publications


# count all different publications
async def count_publications():
    result = []
    async for publication in publication_collection.aggregate([
        {
            '$group': {
                '_id': '$obj.data.doi'
            }
        }, {
            '$count': 'count'
        }
    ]):
        result.append(publication)
    return result


# get count of group by field
async def group_count_publications(field='obj.data.doi', limit=10):
    publications = []
    async for publication in publication_collection.aggregate([
        {
            '$group': {
                '_id': '$' + field,
                'count': {
                    '$sum': 1
                }
            }
        }, {
            '$sort': {
                'count': -1
            }
        }, {
            '$limit': int(limit)
        }
    ]):
        publications.append(publication_helper(publication))
    return publications


# get count of types
async def get_types(id):
    publications = []

    query = [
        {
            '$addFields': {
                'type': {
                    '$ifNull': [
                        '$subj.data.referenced_tweets.type', 'tweet'
                    ]
                }
            }
        }, {
            '$unwind': {
                'path': '$type',
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$group': {
                '_id': '$type',
                'count': {
                    '$sum': 1
                }
            }
        }, {
            '$sort': {
                'count': -1
            }
        }
    ]
    if id:
        query = [
            {
                '$match': {
                    'obj.data.id': str(id)
                }
            }, {
                '$addFields': {
                    'type': {
                        '$ifNull': [
                            '$subj.data.referenced_tweets.type', 'tweet'
                        ]
                    }
                }
            }, {
                '$unwind': {
                    'path': '$type',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$group': {
                    '_id': '$type',
                    'count': {
                        '$sum': 1
                    }
                }
            }, {
                '$sort': {
                    'count': -1
                }
            }
        ]

    async for publication in publication_collection.aggregate(query):
        publications.append(publication_helper(publication))
    return publications


# get count sources
async def get_sources(id):
    publications = []

    query = [
        {
            '$group': {
                '_id': '$subj.data.source',
                'count': {
                    '$sum': 1
                }
            }
        }, {
            '$sort': {
                'count': -1
            }
        }
    ]
    if id:
        query = [
            {
                '$match': {
                    'obj.data.id': str(id)
                }
            }, {
                '$group': {
                    '_id': '$subj.data.source',
                    'count': {
                        '$sum': 1
                    }
                }
            }, {
                '$sort': {
                    'count': -1
                }
            }
        ]
    query.append({
        '$limit': 20
    })

    async for publication in publication_collection.aggregate(query):
        publications.append(publication_helper(publication))
    return publications


# get count sources
async def get_top_authors(id, original=False):
    publications = []
    query = []
    if id:
        query.append(
            {
                '$match': {
                    'obj.data.id': str(id)
                }
            })

    query.extend([
        {
            '$match': {
                'subj.data.in_reply_to_user_id': {
                    '$exists': False
                }
            }
        },
        {
            '$match': {
                'subj.data.referenced_tweets': {
                    '$exists': False
                }
            }
        }])

    if original:
        query.extend([
            {
                '$group': {
                    '_id': '$subj.data.author_id',
                    'total': {
                        '$sum': 1
                    }
                }
            },
            {
                '$sort': {
                    'total': -1
                }
            }
        ])

    query.append(
        {
            '$limit': 20
        })

    async for publication in publication_collection.aggregate(query):
        publications.append(publication_helper(publication))
    return publications


# get count languages
async def get_top_lang(id):
    publications = []
    query = []
    if id:
        query.append(
            {
                '$match': {
                    'obj.data.id': str(id)
                }
            })
    query.extend([
        {
            '$group': {
                '_id': '$subj.data.lang',
                'count': {
                    '$sum': 1
                }
            }
        },
        {
            '$sort': {
                'count': -1
            }
        },
        {
            '$limit': 20
        }])

    async for publication in publication_collection.aggregate(query):
        publications.append(publication_helper(publication))
    return publications


# get count languages
async def get_top_entities(id):
    publications = []
    query = []
    if id:
        query.append(
            {
                '$match': {
                    'obj.data.id': str(id)
                }
            })
    query.extend([{
        '$unwind': {
            'path': '$subj.data.context_annotations',
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$subj.data.context_annotations.entity.name',
            'count': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            'count': -1
        }
    }, {
        '$limit': 20
    }
    ])

    async for publication in publication_collection.aggregate(query):
        publications.append(publication_helper(publication))
    return publications


# get count hashtags
async def get_top_hashtags(id):
    publications = []
    query = []
    if id:
        query.append(
            {
                '$match': {
                    'obj.data.id': str(id)
                }
            })
    query.extend([
        {
            '$unwind': {
                'path': '$subj.data.entities.hashtags',
                'preserveNullAndEmptyArrays': False
            }
        }, {
            '$group': {
                '_id': '$subj.data.entities.hashtags.tag',
                'count': {
                    '$sum': 1
                }
            }
        }, {
            '$sort': {
                'count': -1
            }
        },
        {
            '$limit': 20
        }
    ])

    async for publication in publication_collection.aggregate(query):
        publications.append(publication_helper(publication))
    return publications


# get hour binned periodic count
async def get_tweet_time_of_day(id):
    publications = []
    query = []
    if id:
        query.append(
            {
                '$match': {
                    'obj.data.id': str(id)
                }
            })
    query.extend([
        {
            '$addFields': {
                'created_at': {
                    '$toDate': '$occurred_at'
                }
            }
        }, {
            '$group': {
                '_id': {
                    '$hour': '$created_at'
                },
                'total': {
                    '$sum': 1
                }
            }
        }, {
            '$sort': {
                '_id': 1
            }
        }
    ])

    async for publication in publication_collection.aggregate(query):
        publications.append(publication_helper(publication))
    return publications
