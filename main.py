import logging

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from app.routers.event import router as EventRouter
from app.routers.publication import router as PublicationRouter

from starlette.requests import Request
from starlette.staticfiles import StaticFiles
from starlette.responses import RedirectResponse, JSONResponse, HTMLResponse

app = FastAPI()

app.mount("/static", StaticFiles(directory="amba-streams-frontend/public"), name="static")

app.include_router(EventRouter, tags=["Event"], prefix="/api/event")
app.include_router(PublicationRouter, tags=["Publication"], prefix="/api/publication")

# @app.get("/app")
# def root():
#     with open('/vue/dist/index.html') as f:
#         logging.warning("errors " + f.errors)
#         return HTMLResponse(content=f.read(), status_code=200)\

@app.get("/app")
def root():
    with open('amba-streams-frontend/public/index.html') as f:
        return HTMLResponse(content=f.read(), status_code=200)