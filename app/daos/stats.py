import motor.motor_asyncio
from collections import Counter

config = {
    'mongo_url': "mongodb://root:example@mongo_db:27017/",
    'mongo_client': "events",
    'mongo_collection': "processed",
}

MONGO_DETAILS = config['mongo_url']
motor_client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_DETAILS)
database = motor_client[config['mongo_client']]
stats_collection = database[config['mongo_collection']]


def stats_helper(stats) -> dict:
    return stats

# deprecated only testing
async def words_from_tweet():
    result = []
    async for words in stats_collection.aggregate([
        {
            '$match': {
                'obj.data.doi': '2419563916'
            }
        }, {
            '$project': {
                'words': {
                    '$split': [
                        '$subj.data.text', ' '
                    ]
                }
            }
        }
    ]):
        result = result + words['words']
    # print(result)
    return Counter(result)
    # return result


# get count of group by field
async def group_count_publications(field='obj.data.doi', limit=10):
    publications = []
    async for publication in stats_collection.aggregate([
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
        publications.append(stats_helper(publication))
    return publications


# count all different publications
async def count_publications():
    result = []
    async for publication in stats_collection.aggregate([
        {
            '$group': {
                '_id': 'doi'
            }
        }, {
            '$count': 'count'
        }
    ]):
        result.append(publication)
    return result

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
                    'obj.data.doi': str(id)
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

    async for publication in stats_collection.aggregate(query):
        publications.append(stats_helper(publication))
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
                    'obj.data.doi': str(id)
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

    async for publication in stats_collection.aggregate(query):
        publications.append(stats_helper(publication))
    return publications


# get count sources
async def get_top_authors(id, original=False):
    publications = []
    query = []
    if id:
        query.append(
            {
                '$match': {
                    'obj.data.doi': str(id)
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

    async for publication in stats_collection.aggregate(query):
        publications.append(stats_helper(publication))
    return publications


# get count languages
async def get_top_lang(id):
    publications = []
    query = []
    if id:
        query.append(
            {
                '$match': {
                    'obj.data.doi': str(id)
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

    async for publication in stats_collection.aggregate(query):
        publications.append(stats_helper(publication))
    return publications


# get count languages
async def get_top_entities(id):
    publications = []
    query = []
    if id:
        query.append(
            {
                '$match': {
                    'obj.data.doi': str(id)
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

    async for publication in stats_collection.aggregate(query):
        publications.append(stats_helper(publication))
    return publications


# get count hashtags
async def get_top_hashtags(id):
    publications = []
    query = []
    if id:
        query.append(
            {
                '$match': {
                    'obj.data.doi': str(id)
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

    async for publication in stats_collection.aggregate(query):
        publications.append(stats_helper(publication))
    return publications


# get hour binned periodic count
async def get_tweet_time_of_day(id):
    publications = []
    query = []
    if id:
        query.append(
            {
                '$match': {
                    'obj.data.doi': str(id)
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

    async for publication in stats_collection.aggregate(query):
        publications.append(stats_helper(publication))
    return publications

