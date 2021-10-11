from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os
import sentry_sdk
from sentry_sdk.integrations.asgi import SentryAsgiMiddleware
from app.routers.publication import router as PublicationRouter
from app.routers.stats import router as StatsRouter
from app.routers.field_of_study import router as FieldOfStudyRouter
from app.routers.author import router as AuthorRouter
from starlette.responses import JSONResponse
from app.daos.database import query_api

from app.daos.stats import (
    system_running_check,
)

SENTRY_DSN = os.environ.get('SENTRY_DSN')
SENTRY_TRACE_SAMPLE_RATE = os.environ.get('SENTRY_TRACE_SAMPLE_RATE')
sentry_sdk.init(
    dsn=SENTRY_DSN,
    traces_sample_rate=SENTRY_TRACE_SAMPLE_RATE
)

description = """
ambalytics analysis streams api allows you to retrieve data which hast been collected, processed and analyzed by
the streaming pipeline.

## Publications
Get Publication data.
        
## Authors
Get Author data.

## Fields of Study
Get Field of Study data.

## Stats
Get statistical numbers, data and more for Publications.

## default
Utilities.
"""

app = FastAPI(
    title="ambalytics analysis streams api",
    description=description,
    version="0.0.1",
    terms_of_service="https://ambalytics.com/",
    contact={
        "name": "Lukas Jesche",
        "url": "https://ambalytics.com/",
        "email": "lukas.jesche.se@gmail.com",
    },
    license_info={
        "name": "Open Data Commons Attribution License 1.0",
        "url": "https://opendatacommons.org/licenses/by/1-0/",
    },
)

try:
    app.add_middleware(SentryAsgiMiddleware)
except Exception:
    # pass silently if the Sentry integration failed
    pass

origins = [
    "http://localhost",
    "http://localhost:8080",
    "http://localhost:8081",
    "https://analysis.ambalytics.cloud",
    "https://mira.ambalytics.com",
    "https://ambalytics.com",
    "*"
]

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


@app.get("/available", response_description="available", summary="Check if api is available.")
def is_api_available():
    """
    Checks if the api is running as expected.
    It returns 'ok' normally, if there is to little data in the last few minutes it will return 'not running'
    """
    return JSONResponse(content=system_running_check(query_api))
