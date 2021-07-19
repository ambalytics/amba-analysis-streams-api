import json
import logging
import time
from typing import List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from update_consumer import UpdateConsumer

app = FastAPI()

html = """
<!DOCTYPE html>
<html>
    <head>
        <title>Tweets</title>
    </head>
    <body>
        <h1>WebSocket Tweets from Kafka</h1>
        <textarea id='tweets'>
        </textarea>
        <script>
            var ws = new WebSocket("ws://localhost:8080/ws");
            ws.onmessage  = function(event) {
                console.log(event);
                var tweets = document.getElementById('tweets');
                tweets.value = event.data;
            };
        </script>
    </body>
</html>
"""


class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except WebSocketDisconnect:
                self.active_connections.remove(connection)
                print('websocket disconnect')


manager = ConnectionManager()

@app.on_event("startup")
async def startup_event():
    logging.warning("start Mongo DB connector")
    time.sleep(15)
    # how to connect this best
    update_consumer = UpdateConsumer(1, manager)




@app.get("/")
async def get():
    print('browser connected')
    return HTMLResponse(html)


# this is where the js can connect
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    print('websocket connected')
