from fastapi import APIRouter, Body
from fastapi.encoders import jsonable_encoder

from app.daos.event import (
    retrieve_event,
    retrieve_events,
    retrieve_newest_events,
)
from app.models.event import (
    ErrorResponseModel,
    ResponseModel,
    EventSchema,
    UpdateEventModel,
)

router = APIRouter()


@router.get("/", response_description="events retrieved")
async def get_events():
    events = await retrieve_events()
    if events:
        return ResponseModel(events, "events data retrieved successfully")
    return ResponseModel(events, "Empty list returned")


@router.get("/newest", response_description="newest events retrieved")
async def get_newest_events():
    events = await retrieve_newest_events()
    if events:
        return ResponseModel(events, "newest events data retrieved successfully")
    return ResponseModel(events, "Empty list returned")


@router.get("/{id}", response_description="event data retrieved")
async def get_event_data(id):
    event = await retrieve_event(id)
    if event:
        return ResponseModel(event, "event data retrieved successfully")
    return ErrorResponseModel("An error occurred.", 404, "event doesn't exist.")

