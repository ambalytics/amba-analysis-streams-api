import json
import logging
import os

import typing
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

from app.routers.publication import router as PublicationRouter
from app.routers.stats import router as StatsRouter
from app.routers.field_of_study import router as FieldOfStudyRouter
from app.routers.author import router as AuthorRouter
from sqlalchemy.orm import sessionmaker
from starlette.endpoints import WebSocketEndpoint
from starlette.requests import Request
from starlette.staticfiles import StaticFiles
from starlette.responses import RedirectResponse, JSONResponse, HTMLResponse
from starlette.websockets import WebSocket, WebSocketDisconnect
from aiokafka import AIOKafkaConsumer
import asyncio
from app.daos.database import query_api

from app.daos.stats import (
    system_running_check,
)

app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:8080",
    "http://localhost:8081",
    "https://analysis.ambalytics.cloud",
    "https://mira.ambalytics.com",
    "https://ambalytics.com",
    "*"
]

# todo check
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(PublicationRouter, tags=["Publication"], prefix="/api/publication")
app.include_router(FieldOfStudyRouter, tags=["FieldOfStudy"], prefix="/api/fieldOfStudy")
app.include_router(AuthorRouter, tags=["Author"], prefix="/api/author")
app.include_router(StatsRouter, tags=["Stats"], prefix="/api/stats")


@app.get("/available", response_description="available")
def is_api_available():
    return JSONResponse(content=system_running_check(query_api))
