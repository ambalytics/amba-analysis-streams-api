from typing import Optional

from pydantic import BaseModel, EmailStr, Field


class StatsSchema(BaseModel):

    class Config:
        schema_extra = {
            "example": {
            }
        }


def ResponseModel(data, message):
    return {
        "data": data,
        "code": 200,
        "message": message,
    }


def ErrorResponseModel(error, code, message):
    return {"error": error, "code": code, "message": message}