import time
from datetime import datetime
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
import os
import sentry_sdk
from sentry_sdk.integrations.asgi import SentryAsgiMiddleware
from app.routers.publication import router as PublicationRouter
from app.routers.stats import router as StatsRouter
from app.routers.field_of_study import router as FieldOfStudyRouter
from app.routers.author import router as AuthorRouter
from starlette.responses import JSONResponse
from app.daos.database import SessionLocal, engine, query_api, write_api, org

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
    version="0.9.1",
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
    "https://trends.ambalytics.com",
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


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    point = {
        "measurement": "response_time",
        "tags": {
            "path": request.url.path
        },
        "fields": {
            'response_time': int(process_time * 1000),
            'url': str(request.url)
        },
        "time": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}

    print(point)
    write_api.write('api_monitor', org, [point])
    return response


app.include_router(PublicationRouter, tags=["Publication"], prefix="/api/trend/publication")
app.include_router(FieldOfStudyRouter, tags=["FieldOfStudy"], prefix="/api/trend/fieldOfStudy")
app.include_router(AuthorRouter, tags=["Author"], prefix="/api/trend/author")
app.include_router(StatsRouter, tags=["Stats"], prefix="/api/trend/stats")


@app.get("/api/trend/available", response_description="available", summary="Check if api is available.")
def is_api_available():
    """
    Checks if the api is running as expected.
    It returns 'ok' normally, if there is to little data in the last few minutes it will return 'not running'
    """
    return JSONResponse(content=system_running_check(query_api))
