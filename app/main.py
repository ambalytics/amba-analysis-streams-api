import json
import logging

import typing
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

from app.routers.event import router as EventRouter
from app.routers.publication import router as PublicationRouter
from app.routers.stats import router as StatsRouter
from starlette.endpoints import WebSocketEndpoint

from starlette.requests import Request
from starlette.staticfiles import StaticFiles
from starlette.responses import RedirectResponse, JSONResponse, HTMLResponse
from starlette.websockets import WebSocket
from aiokafka import AIOKafkaConsumer
import asyncio

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

# app.mount("/static", StaticFiles(directory="/vue/dist"), name="static")

app.include_router(PublicationRouter, tags=["Publication"], prefix="/api/publication")
app.include_router(EventRouter, tags=["Event"], prefix="/api/event")
app.include_router(StatsRouter, tags=["Stats"], prefix="/api/stats")

@app.websocket_route("/live")
class WebsocketConsumer(WebSocketEndpoint):
    async def on_connect(self, websocket: WebSocket) -> None:

        topicname = 'events_aggregated'

        await websocket.accept()
        await websocket.send_json({"Message": "connected"})

        loop = asyncio.get_event_loop()
        self.consumer = AIOKafkaConsumer(
            topicname,
            loop=loop,
            bootstrap_servers='kafka:9092',
            enable_auto_commit=False,
        )

        await self.consumer.start()

        async for msg in self.consumer:
            await self.on_receive(websocket, json.loads(msg.value.decode('utf-8')))
            print("consumed: ", msg.value)

        print("connected")

    async def on_disconnect(self, websocket: WebSocket, close_code: int) -> None:
        self.consumer_task.cancel()
        await self.consumer.stop()
        print(f"counter: {self.counter}")
        print("disconnected")
        print("consumer stopped")

    async def on_receive(self, websocket: WebSocket, data: typing.Any) -> None:
        print('receive')
        await websocket.send_json({"Message": data})
