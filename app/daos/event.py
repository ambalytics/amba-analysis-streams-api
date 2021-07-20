import motor.motor_asyncio
from bson.objectid import ObjectId

config = {
    'mongo_url': "mongodb://root:example@mongo_db:27017/",
    'mongo_client': "events",
    'mongo_collection': "linked",
}

MONGO_DETAILS = config['mongo_url']
motor_client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_DETAILS)
database = motor_client[config['mongo_client']]
event_collection = database[config['mongo_collection']]


def event_helper(event) -> dict:
    return event


# Retrieve all events present in the database
async def retrieve_events():
    events = []
    async for event in event_collection.find():
        events.append(event_helper(event))
    return events


# Retrieve a event with a matching ID
async def retrieve_event(id: str) -> dict:
    event = await event_collection.find_one({"_id": ObjectId(id)})
    if event:
        return event_helper(event)


# return newest events
async def retrieve_newest_events():
    events = []
    async for event in event_collection.aggregate([
        {
            '$sort': {
                'timestamp': -1
            }
        }, {
            '$limit': 10
        }
    ]):
        events.append(event_helper(event))
    return events
