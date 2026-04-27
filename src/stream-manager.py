import os
import json
import asyncio
import datetime
import aiomqtt
import httpx
import logging

from typing import Dict, Set, Optional
from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from contextlib import asynccontextmanager

import uvicorn
from starlette.websockets import WebSocket as StarletteWS
StarletteWS._check_origin = lambda self, scope: True

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
level_name = os.getenv('LOGGING_LEVEL', 'INFO').upper()
logging.basicConfig(level=getattr(logging, level_name), format='[%(levelname)s] %(message)s')
logger = logging.getLogger('stream-service')

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
mqtt_broker        = os.getenv('MQTT_BROKER')
agent_callback_port = os.getenv('AGENT_CALLBACK_PORT', '8080')
agent_callback_path = os.getenv('AGENT_CALLBACK_PATH', '/ingest')
max_topics_per_session = int(os.getenv('MAX_TOPICS_PER_SESSION', '20'))

if mqtt_broker is None:
    logger.warning("MQTT_BROKER env var not set. Subscriptions will fail.")
else:
    logger.info(f"MQTT_BROKER set to {mqtt_broker}")

# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------
class SubscribeRequest(BaseModel):
    session_id: str
    topic: str
    consumer_type: str          # "browser" or "agent"
    callback_url: Optional[str] = None   # agent only, overrides auto-constructed URL

class UnsubscribeRequest(BaseModel):
    session_id: str
    topic: str

# ---------------------------------------------------------------------------
# Session state
# Tracks for each session_id:
#   - consumer_type
#   - callback_url (agents)
#   - active WebSocket connections (browsers)
#   - which topics are subscribed
# ---------------------------------------------------------------------------
class SessionState:
    def __init__(self, session_id: str, consumer_type: str, callback_url: Optional[str]):
        self.session_id    = session_id
        self.consumer_type = consumer_type
        self.callback_url  = callback_url
        self.topics: Set[str] = set()
        self.websockets: Set[WebSocket] = set()
        self.ws_lock  = asyncio.Lock()

# ---------------------------------------------------------------------------
# Shared MQTT Manager
# One persistent MQTT connection for the whole service.
# Routes incoming messages to the correct session by topic.
# ---------------------------------------------------------------------------
class MQTTStreamManager:
    def __init__(self, broker_url: str):
        self.broker_url = broker_url

        # session_id -> SessionState
        self.sessions: Dict[str, SessionState] = {}
        # topic -> set of session_ids  (many sessions can share a topic)
        self.topic_sessions: Dict[str, Set[str]] = {}

        self.state_lock = asyncio.Lock()
        self._listener_task: Optional[asyncio.Task] = None
        self._mqtt_client: Optional[aiomqtt.Client] = None
        self._pending_subscribes: asyncio.Queue = asyncio.Queue()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self):
        self._listener_task = asyncio.create_task(self._shared_listener())
        logger.info("Shared MQTT listener started.")

    async def stop(self):
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        logger.info("Shared MQTT listener stopped.")

    # ------------------------------------------------------------------
    # Public API called by HTTP endpoints
    # ------------------------------------------------------------------
    async def register_session(
        self,
        session_id: str,
        consumer_type: str,
        callback_url: Optional[str]
    ) -> bool:
        """Register a session if not already known. Returns True if new."""
        async with self.state_lock:
            if session_id not in self.sessions:
                self.sessions[session_id] = SessionState(session_id, consumer_type, callback_url)
                return True
            return False

    async def subscribe(self, session_id: str, topic: str) -> str:
        async with self.state_lock:
            if session_id not in self.sessions:
                return "Session not registered."

            state = self.sessions[session_id]
            if len(state.topics) >= max_topics_per_session:
                return f"Subscription limit reached ({max_topics_per_session} topics max)."
            if topic in state.topics:
                return f"Already subscribed to {topic}."

            state.topics.add(topic)

            if topic not in self.topic_sessions:
                self.topic_sessions[topic] = set()
                # Tell the listener to subscribe on the broker
                await self._pending_subscribes.put(("subscribe", topic))

            self.topic_sessions[topic].add(session_id)

        logger.info(f"Session {session_id} subscribed to {topic}")
        return f"Subscribed to {topic}. Data streaming."

    async def unsubscribe(self, session_id: str, topic: str) -> str:
        async with self.state_lock:
            if session_id not in self.sessions:
                return "Session not registered."

            state = self.sessions[session_id]
            if topic not in state.topics:
                return "No active subscription found."

            state.topics.discard(topic)

            if topic in self.topic_sessions:
                self.topic_sessions[topic].discard(session_id)
                if not self.topic_sessions[topic]:
                    del self.topic_sessions[topic]
                    await self._pending_subscribes.put(("unsubscribe", topic))

        logger.info(f"Session {session_id} unsubscribed from {topic}")
        return f"Unsubscribed from {topic}."

    async def cleanup_session(self, session_id: str):
        async with self.state_lock:
            state = self.sessions.pop(session_id, None)
            if not state:
                return

            for topic in list(state.topics):
                if topic in self.topic_sessions:
                    self.topic_sessions[topic].discard(session_id)
                    if not self.topic_sessions[topic]:
                        del self.topic_sessions[topic]
                        await self._pending_subscribes.put(("unsubscribe", topic))

        logger.info(f"Session {session_id} cleaned up.")
        
    async def get_all_subscriptions(self) -> dict:
        async with self.state_lock:
            return {
                "sessions": [
                    {
                        "session_id": state.session_id,
                        "consumer_type": state.consumer_type,
                        "callback_url": state.callback_url,
                        "topics": sorted(state.topics),
                    }
                    for state in self.sessions.values()
                ],
                "topics": {
                    topic: sorted(list(session_ids))
                    for topic, session_ids in self.topic_sessions.items()
                }
            }
            
    async def get_session_subscriptions(self, session_id: str) -> Optional[dict]:
        async with self.state_lock:
            state = self.sessions.get(session_id)
            if not state:
                return None
            return {
                "session_id": state.session_id,
                "consumer_type": state.consumer_type,
                "callback_url": state.callback_url,
                "topics": sorted(state.topics)
            }

    # ------------------------------------------------------------------
    # WebSocket management (browser sessions)
    # ------------------------------------------------------------------
    async def add_websocket(self, session_id: str, ws: WebSocket):
        async with self.state_lock:
            state = self.sessions.get(session_id)
        if state:
            async with state.ws_lock:
                state.websockets.add(ws)

    async def remove_websocket(self, session_id: str, ws: WebSocket):
        async with self.state_lock:
            state = self.sessions.get(session_id)
        if state:
            async with state.ws_lock:
                state.websockets.discard(ws)

    # ------------------------------------------------------------------
    # Shared MQTT listener — one connection for all topics
    # ------------------------------------------------------------------
    async def _shared_listener(self):
        while True:
            try:
                async with aiomqtt.Client(self.broker_url) as client:
                    self._mqtt_client = client

                    # Re-subscribe to all currently active topics after reconnect
                    async with self.state_lock:
                        active_topics = list(self.topic_sessions.keys())
                    for topic in active_topics:
                        await client.subscribe(topic)
                        logger.info(f"(Re)subscribed to broker topic: {topic}")

                    # Process incoming messages and pending subscribe/unsubscribe requests
                    async with asyncio.TaskGroup() as tg:
                        tg.create_task(self._message_loop(client))
                        tg.create_task(self._pending_loop(client))

            except* asyncio.CancelledError:
                raise
            except* Exception as e:
                logger.error(f"MQTT connection error: {e}. Reconnecting in 5s...")
                self._mqtt_client = None
                await asyncio.sleep(5)

    async def _message_loop(self, client: aiomqtt.Client):
        async for message in client.messages:
            topic   = str(message.topic)
            payload = message.payload.decode()
            data    = {
                "type":      "mqtt_data",
                "topic":     topic,
                "payload":   payload,
                "timestamp": datetime.datetime.now().isoformat()
            }
            async with self.state_lock:
                session_ids = list(self.topic_sessions.get(topic, set()))

            for session_id in session_ids:
                async with self.state_lock:
                    state = self.sessions.get(session_id)
                if state:
                    await self._forward(state, data)

    async def _pending_loop(self, client: aiomqtt.Client):
        """Apply pending subscribe/unsubscribe requests to the live MQTT client."""
        while True:
            action, topic = await self._pending_subscribes.get()
            try:
                if action == "subscribe":
                    await client.subscribe(topic)
                    logger.info(f"MQTT subscribed: {topic}")
                elif action == "unsubscribe":
                    await client.unsubscribe(topic)
                    logger.info(f"MQTT unsubscribed: {topic}")
            except Exception as e:
                logger.error(f"Failed to {action} {topic}: {e}")

    # ------------------------------------------------------------------
    # Forwarding — browser via WebSocket, agent via HTTP callback
    # ------------------------------------------------------------------
    async def _forward(self, state: SessionState, data: dict):
        if state.consumer_type == "browser":
            await self._forward_ws(state, data)
        elif state.consumer_type == "agent":
            await self._forward_http(state, data)

    async def _forward_ws(self, state: SessionState, data: dict):
        async with state.ws_lock:
            dead = set()
            for ws in list(state.websockets):
                try:
                    await ws.send_json(data)
                except Exception:
                    dead.add(ws)
            state.websockets -= dead

    async def _forward_http(self, state: SessionState, data: dict):
        if not state.callback_url:
            logger.warning(f"Agent session {state.session_id} has no callback_url, dropping message.")
            return
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.post(state.callback_url, json=data)
        except Exception as e:
            logger.error(f"HTTP callback failed for session {state.session_id}: {e}")


# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    manager = MQTTStreamManager(mqtt_broker)
    app.state.manager = manager
    await manager.start()
    yield
    await manager.stop()

app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

# ---------------------------------------------------------------------------
# HTTP endpoints (called by MCP-Client)
# ---------------------------------------------------------------------------
@app.post("/subscribe")
async def subscribe(req: SubscribeRequest, request: Request):
    manager: MQTTStreamManager = app.state.manager

    # Resolve callback URL for agents
    callback_url = None
    if req.consumer_type == "agent":
        if req.callback_url:
            callback_url = req.callback_url
        else:
            origin_ip = request.client.host
            callback_url = f"http://{origin_ip}:{agent_callback_port}{agent_callback_path}"
            logger.info(f"Auto-constructed callback URL for agent: {callback_url}")

    elif req.consumer_type != "browser":
        return JSONResponse(status_code=400, content={"error": f"Unknown consumer_type: {req.consumer_type}"})

    await manager.register_session(req.session_id, req.consumer_type, callback_url)
    result = await manager.subscribe(req.session_id, req.topic)
    return {"session_id": req.session_id, "result": result}

@app.post("/unsubscribe")
async def unsubscribe(req: UnsubscribeRequest):
    manager: MQTTStreamManager = app.state.manager
    result = await manager.unsubscribe(req.session_id, req.topic)
    return {"session_id": req.session_id, "result": result}

@app.post("/cleanup/{session_id}")
async def cleanup(session_id: str):
    manager: MQTTStreamManager = app.state.manager
    await manager.cleanup_session(session_id)
    return {"session_id": session_id, "result": "cleaned up"}

# ---------------------------------------------------------------------------
# WebSocket endpoint (browsers connect here directly after subscribing)
# ---------------------------------------------------------------------------
@app.websocket("/ws/{session_id}")
async def websocket_endpoint(websocket: WebSocket, session_id: str):
    manager: MQTTStreamManager = app.state.manager

    await websocket.accept()
    await manager.add_websocket(session_id, websocket)
    logger.info(f"Browser WebSocket connected for session {session_id}")

    try:
        while True:
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
            except asyncio.TimeoutError:
                # Client is quiet — send a ping to verify it's still alive
                try:
                    await websocket.send_json({"type": "ping"})
                except Exception:
                    break  # Can't reach client, treat as disconnect
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        await manager.remove_websocket(session_id, websocket)
        await manager.cleanup_session(session_id)
        logger.info(f"Browser WebSocket disconnected for session {session_id}")
# ---------------------------------------------------------------------------
# Getting infos about current subscriptions 
# ---------------------------------------------------------------------------      
        
@app.get("/subscriptions")
async def list_subscriptions():
    manager: MQTTStreamManager = app.state.manager
    return await manager.get_all_subscriptions()

@app.get("/subscriptions/{session_id}")
async def get_session_subscriptions(session_id: str):
    manager: MQTTStreamManager = app.state.manager
    result = await manager.get_session_subscriptions(session_id)
    if result is None:
        return JSONResponse(status_code=404, content={"error": "Session not found"})
    return result  

# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------
@app.get("/health")
async def health():
    return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8002")))