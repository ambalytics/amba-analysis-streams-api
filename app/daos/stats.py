import motor.motor_asyncio
from collections import Counter

config = {
    'mongo_url': "mongodb://root:example@mongo_db:27017/",
    'mongo_client': "events",
    'mongo_collection': "linked",
}

MONGO_DETAILS = config['mongo_url']
motor_client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_DETAILS)
database = motor_client[config['mongo_client']]
event_collection = database[config['mongo_collection']]



async def words_from_tweet():
    result = []
    async for words in event_collection.aggregate([
        {
            '$match': {
                'obj.data.id': '2419563916'
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
