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
mqtt_broker            = os.getenv('MQTT_BROKER')
agent_server_url       = os.getenv('AGENT_SERVER_URL', 'http://localhost:8000/agents').rstrip('/')
max_topics_per_session = int(os.getenv('MAX_TOPICS_PER_SESSION', '20'))

if mqtt_broker is None:
    logger.warning("MQTT_BROKER env var not set. Subscriptions will fail.")
else:
    logger.info(f"MQTT_BROKER set to {mqtt_broker}")

# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------
class SubscribeRequest(BaseModel):
    session_id: str # Session of the user that request subscription
    topic: str # MQTT topic to subscribe to
    text: str # Text that is used as instruction for the agent                         
    purposes: list[str] = [] # PBAC purposes 
    memory_window: int = 5 
    agent_server_url: Optional[str] = None  # overrides AGENT_SERVER_URL env var
    
class BrowserSubscribeRequest(BaseModel):
    session_id: str
    topic: str
    purposes: list[str] = []

class UnsubscribeRequest(BaseModel):
    session_id: str
    topic: str
    

# ---------------------------------------------------------------------------
# Session state
# Tracks for each session_id:
#   - which topics are subscribed
#   - agent config (text, purposes, memory_window, agent_server_url)
#   - agent_id returned by /agents after first message triggers creation
# ---------------------------------------------------------------------------
class SessionState:
    def __init__(self, session_id: str, text: str, purposes: list,
                 memory_window: int, agent_server_url: str):
        self.session_id             = session_id
        self.topics: Set[str]       = set()
        self.text                   = text
        self.purposes               = purposes
        self.memory_window          = memory_window
        self.agent_server_url       = agent_server_url
        self.agent_id: Optional[str] = None  # set after /agents responds on first message
        # browser options
        self.webscokets: Set[WebSocket] = set()
        self.ws_lock = asyncio.Lock()
        

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
    async def register_session(self, session_id: str, text: str, purposes: list,
                                memory_window: int, agent_server_url: str,
                                consumer_type: str = "agent") -> bool:
        async with self.state_lock:
            if session_id not in self.sessions:
                self.sessions[session_id] = SessionState(
                    session_id, text, purposes, memory_window, agent_server_url, consumer_type
                )
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

        # Notify agent-manager to stop the agent (delete endpoint )
        if state.agent_id:
            url = f"{state.agent_server_url}/{state.agent_id}"
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    await client.delete(url)
                    logger.info(f"Agent {state.agent_id} deleted for session {session_id}")
            except Exception as e:
                logger.error(f"Failed to delete agent {state.agent_id}: {e}")

        logger.info(f"Session {session_id} cleaned up.")

    async def get_all_subscriptions(self) -> dict:
        async with self.state_lock:
            return {
                "sessions": [
                    {
                        "session_id": state.session_id,
                        "agent_id":   state.agent_id,
                        "topics":     sorted(state.topics),
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
                "agent_id":   state.agent_id,
                "topics":     sorted(state.topics),
            }

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
                raise asyncio.CancelledError()
            except* Exception as eg:
                for e in eg.exceptions:
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
    # Forwarding — to agent-manager 
    # ------------------------------------------------------------------
    async def _forward(self, state: SessionState, data: dict):
        if state.consumer_type == "browser":
            await self._forward_ws(state, data)
        else:
            await self._forward_to_agent(state, data)

    async def _forward_to_agent(self, state: SessionState, data: dict):
        url = state.agent_server_url

        if state.agent_id is None:
            # Agent not created yet — create it now on first incoming message
            combined_text = f"{state.text}\n\nData: {data['payload']}"
            create_payload = {
                "intervalMs":   0,
                "runOnce":      True,
                "text":         combined_text,
                "purposes":     state.purposes,
                "memoryWindow": state.memory_window,
            }
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    resp = await client.post(url, json=create_payload)
                    resp.raise_for_status()
                    state.agent_id = resp.json()["id"]
                    logger.info(f"Agent {state.agent_id} created for session {state.session_id}")
            except Exception as e:
                logger.error(f"Agent creation failed for session {state.session_id}: {e}. Cleaning up.")
                await self.cleanup_session(state.session_id)
        else:
            # Agent already running — forward the new datapoint
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    resp = await client.post(
                        f"{url}/{state.agent_id}",
                        json={"datapoint": data["payload"]}
                    )
                    resp.raise_for_status()
                    logger.info(f"Datapoint forwarded to agent {state.agent_id}")
            except Exception as e:
                logger.error(f"Datapoint forward failed for agent {state.agent_id}: {e}")
    
    async def _forward_ws(self, state: SessionState, data: dict):
        async with state.ws_lock:
            dead = set()
            for ws in list(state.websockets):
                try:
                    await ws.send_json(data)
                except Exception:
                    dead.add(ws)
            state.websockets -= dead

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
# HTTP endpoints
# ---------------------------------------------------------------------------
@app.post("/subscribe")
async def subscribe(req: SubscribeRequest):
    manager: MQTTStreamManager = app.state.manager

    url = req.agent_server_url or agent_server_url

    await manager.register_session(
        session_id=req.session_id,
        text=req.text,
        purposes=req.purposes,
        memory_window=req.memory_window,
        agent_server_url=url,
    )
    result = await manager.subscribe(req.session_id, req.topic)
    return {"session_id": req.session_id, "result": result}

@app.post("/subscribe_browser")
async def subscribe_browser(req: BrowserSubscribeRequest):
    manager: MQTTStreamManager = app.state.manager
    await manager.register_session(
        session_id=req.session_id,
        text="",
        purposes=req.purposes,
        memory_window=0,
        agent_server_url=agent_server_url,
        consumer_type="browser",
    )
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
# WebSocket endpoint 
# ---------------------------------------------------------------------------
@app.websocket("/ws/{session_id}")
async def websocket_endpoint(websocket: WebSocket, session_id: str):
    manager: MQTTStreamManager = app.state.manager
    await websocket.accept()

    async with manager.state_lock:
        state = manager.sessions.get(session_id)
    if not state or state.consumer_type != "browser":
        await websocket.close(code=4004)
        return

    async with state.ws_lock:
        state.websockets.add(websocket)
    logger.info(f"Browser WebSocket connected for session {session_id}")

    try:
        while True:
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
            except asyncio.TimeoutError:
                try:
                    await websocket.send_json({"type": "ping"})
                except Exception:
                    break
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        async with state.ws_lock:
            state.websockets.discard(websocket)
        await manager.cleanup_session(session_id)
        logger.info(f"Browser WebSocket disconnected for session {session_id}")

# ---------------------------------------------------------------------------
# Subscription info endpoints
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
