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
async def get_words(id):
    query = [
        {
            '$match': {
                'obj.data.doi': str(id)
            }
        }, {
            '$project': {
                'doi': '$obj.data.doi',
                'words': '$subj.processed.words'
            }
        }, {
            '$group': {
                '_id': '$doi',
                'words': {
                    '$push': '$words'
                }
            }
        }, {
            '$project': {
                'doi': '$_id',
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
                }
            }
        }, {
            '$project': {
                'doi': '$_id',
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
                }
            }
        }, {
            '$unwind': {
                'path': '$words',
                'preserveNullAndEmptyArrays': False
            }
        }, {
            '$match': {
                'words': {
                    '$regex': '([a-zA-Z])\w+'
                }
            }
        }, {
            '$group': {
                '_id': '$doi',
                'words': {
                    '$push': '$words'
                }
            }
        }
    ]
    publications = []
    async for publication in stats_collection.aggregate(query):
        publications.append(stats_helper(publication))
    return publications[0]


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


# get list of user countries
async def get_country_list(id):
    result = []
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
            '$sort': {
                'timestamp': -1
            }
        }, {
            '$match': {
                'subj.processed.location': {
                    '$exists': True
                }
            }
        }, {
            '$unwind': {
                'path': '$subj.data.includes.users',
                'preserveNullAndEmptyArrays': False
            }
        }, {
            '$project': {
                'loc': '$subj.data.includes.users',
                'p': '$subj.processed'
            }
        }, {
            '$match': {
                'loc.location': {
                    '$exists': True
                }
            }
        }, {
            '$project': {
                'a': '$loc.location',
                'b': '$p.location'
            }
        }, {
            '$group': {
                '_id': '$b',
                'sum': {
                    '$sum': 1
                }
            }
        }, {
            '$sort': {
                'sum': -1
            }
        }
    ])

    async for r in stats_collection.aggregate(query):
        result.append(stats_helper(r))
    return result


# get number of different tweet authors
async def get_tweet_author_count(id):
    result = []
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
            '$project': {
                'a_id': '$subj.data.author_id',
                'id': '$id'
            }
        }, {
            '$group': {
                '_id': '$a_id',
                'count': {
                    '$sum': 1
                }
            }
        }, {
            '$count': 'count'
        }, {
            '$sort': {
                'sum': -1
            }
        }
    ])

    async for r in stats_collection.aggregate(query):
        result.append(stats_helper(r))
    return result


# get number of different tweet authors
async def get_tweet_count(id):
    result = []
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
            '$count': 'count'
        }
    ])

    async for r in stats_collection.aggregate(query):
        result.append(stats_helper(r))
    return result


# get total sum of followers reached
async def get_followers_reached(id):
    result = []
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
            '$project': {
                'a_id': '$subj.data.author_id',
                'f': '$subj.processed.followers'
            }
        }, {
            '$group': {
                '_id': '_id',
                'sum': {
                    '$sum': '$f'
                }
            }
        }, {
            '$sort': {
                'sum': -1
            }
        }
    ])
    async for r in stats_collection.aggregate(query):
        result.append(stats_helper(r))
    return result


# get average score
async def get_total_score(id):
    result = []
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
            '$project': {
                'score': '$subj.processed.score'
            }
        }, {
            '$group': {
                '_id': '_id',
                'sum': {
                    '$sum': '$score'
                }
            }
        }, {
            '$sort': {
                'sum': -1
            }
        }
    ])
    async for r in stats_collection.aggregate(query):
        result.append(stats_helper(r))
    return result


# get tweets binned every 5 minutes
async def get_time_count_binned(id):
    result = []
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
            '$project': {
                'created_at': {
                    '$toDate': '$subj.data.created_at'
                }
            }
        }, {
            '$group': {
                '_id': {
                    '$toDate': {
                        '$subtract': [
                            {
                                '$toLong': '$created_at'
                            }, {
                                '$mod': [
                                    {
                                        '$toLong': '$created_at'
                                    }, 1000 * 60 * 15
                                ]
                            }
                        ]
                    }
                },
                'count': {
                    '$sum': 1
                }
            }
        }, {
            '$sort': {
                '_id': -1
            }
        }
    ])
    async for r in stats_collection.aggregate(query):
        result.append(stats_helper(r))
    return result
