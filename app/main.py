import json
import logging
import os

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
from starlette.websockets import WebSocket, WebSocketDisconnect
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


# class ConnectionManager:
#     consumer = None
#     last_message = {'None'}
#
#     def __init__(self):
#         self.active_connections: typing.List[WebSocket] = []
#
#     async def connect(self, websocket: WebSocket):
#         print('connect')
#         await websocket.accept()
#         self.active_connections.append(websocket)
#         if not self.consumer:
#             loop = asyncio.get_event_loop()
#             bootstrap_servers = os.environ['KAFKA_BOOTRSTRAP_SERVER']
#             topicname = 'events_aggregated'
#             self.consumer = AIOKafkaConsumer(
#                 topicname,
#                 loop=loop,
#                 bootstrap_servers=bootstrap_servers,
#                 enable_auto_commit=False,
#             )
#             await self.consumer.start()
#
#             async for msg in self.consumer:
#                 await self.broadcast(json.loads(msg.value.decode('utf-8')))
#
#     async def disconnect(self, websocket: WebSocket):
#         self.active_connections.remove(websocket)
#         await self.consumer.stop()
#         self.consumer = None
#
#     async def broadcast(self, message: str):
#         self.last_message = message
#         print('broadcast')
#         for connection in self.active_connections:
#             try:
#                 await connection.send_json({"Message": message})
#             except WebSocketDisconnect:
#                 await self.disconnect(connection)
#
#
# manager = ConnectionManager()


# await manager.connect(websocket)

@app.websocket_route("/live")
class WebsocketConsumer(WebSocketEndpoint):
    async def on_connect(self, websocket: WebSocket) -> None:

        topicname = 'events_aggregated'
        bootstrap_servers = os.environ.get('KAFKA_BOOTRSTRAP_SERVER', 'kafka:9092')

        await websocket.accept()

        loop = asyncio.get_event_loop()
        self.consumer = AIOKafkaConsumer(
            topicname,
            loop=loop,
            bootstrap_servers=bootstrap_servers,
            enable_auto_commit=False,
            auto_offset_reset="latest",
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
