import motor.motor_asyncio
from bson.objectid import ObjectId

# todo own client for publciations
config = {
    'mongo_url': "mongodb://root:example@mongo_db:27017/",
    'mongo_client': "events",
    'mongo_collection': "linked",
}

MONGO_DETAILS = config['mongo_url']
motor_client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_DETAILS) # https://medium.com/@bruno.fosados/simple-learn-docker-fastapi-and-vue-js-second-part-backend-fastapi-cec2e1e093a9 instead link
database = motor_client[config['mongo_client']]
publication_collection = database[config['mongo_collection']]


def publication_helper(publication) -> dict:
    return publication


# Retrieve all publications present in the database
async def retrieve_publications():
    publications = []
    async for publication in publication_collection.find():
        publications.append(publication_helper(publication))
    return publications


# Retrieve a publication with a matching ID
async def retrieve_publication(doi: str) -> dict:
    publication = await publication_collection.find_one({"$obj.data.doi": ObjectId(id)})
    if publication:
        return publication_helper(publication)


# Top 10 todo change
async def top_publications():
    publications = []
    async for publication in publication_collection.aggregate([
        {
            '$group': {
                '_id': '$obj.data.doi',
                'count': {
                    '$sum': 1
                }
            }
        }, {
            '$sort': {
                'count': -1
            }
        }, {
            '$limit': 10
        }
    ]):
        publications.append(publication_helper(publication))
    return publications

# get count of group by field
async def count_publications(field = 'obj.data.doi', limit = 10):
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
