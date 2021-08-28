from typing import Optional

from pydantic import BaseModel, EmailStr, Field


class EventSchema(BaseModel):
    _id: str = Field(...)
    # todo

    class Config:
        schema_extra = {
            "example": {
                "_id": "1",
            }
        }


class UpdateEventModel(BaseModel):
    _id: Optional[str]

    class Config:
        schema_extra = {
            "example": {
                "_id": "1",
            }
        }


def ResponseModel(data, message):
    return {
        "data": [data],
        "code": 200,
        "message": message,
    }


def ErrorResponseModel(error, code, message):
    return {"error": error, "code": code, "message": message}