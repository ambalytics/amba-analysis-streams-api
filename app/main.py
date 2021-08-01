import logging

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

from app.routers.event import router as EventRouter
from app.routers.publication import router as PublicationRouter
from app.routers.stats import router as StatsRouter

from starlette.requests import Request
from starlette.staticfiles import StaticFiles
from starlette.responses import RedirectResponse, JSONResponse, HTMLResponse

app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:8080",
    "http://localhost:8081",
    "*"
]

# todo check
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

# app.mount("/static", StaticFiles(directory="/vue/dist"), name="static")

app.include_router(PublicationRouter, tags=["Publication"], prefix="/api/publication")
app.include_router(EventRouter, tags=["Event"], prefix="/api/event")
app.include_router(StatsRouter, tags=["Stats"], prefix="/api/stats")

# @app.get("/app")
# def root():
#     with open('/vue/dist/index.html') as f:
#         logging.warning("errors " + f.errors)
#         return HTMLResponse(content=f.read(), status_code=200)\

# @app.get("/app")
# def root():
#     with open('/vue/dist/index.html') as f:
#         return HTMLResponse(content=f.read(), status_code=200)