import os
import asyncio
import datetime
import httpx
import logging

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

from typing import Dict, Set, Optional
from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from contextlib import asynccontextmanager
from purpose_subscribe_client import PurposeSubscribeClient

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
mqtt_port              = int(os.getenv('MQTT_PORT', '1883'))
mqtt_presub            = os.getenv('MQTT_PRESUB', 'false').lower() == 'true'
agent_server_url       = os.getenv('AGENT_SERVER_URL', 'http://localhost:8000/agents').rstrip('/')
max_topics_per_session = int(os.getenv('MAX_TOPICS_PER_SESSION', '20'))

if mqtt_broker is None:
    logger.warning("MQTT_BROKER env var not set. Subscriptions will fail.")
else:
    logger.info(f"MQTT_BROKER={mqtt_broker}:{mqtt_port}  presub={mqtt_presub}")

# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------
class SubscribeRequest(BaseModel):
    session_id: str
    topic: str
    purpose: str                           
    agent_server_url: Optional[str] = None  # overrides AGENT_SERVER_URL env var

class BrowserSubscribeRequest(BaseModel):
    session_id: str
    topic: str
    purpose: str                          

class UnsubscribeRequest(BaseModel):
    session_id: str
    topic: str


# ---------------------------------------------------------------------------
# Topic subscription record
# ---------------------------------------------------------------------------
class TopicSubscription:
    """Tracks a single topic+purpose pair for a session."""
    def __init__(self, topic: str, purpose: str):
        self.topic   = topic
        self.purpose = purpose


# ---------------------------------------------------------------------------
# Session state — with paho client connection
# ---------------------------------------------------------------------------
class SessionState:
    def __init__(self, session_id: str, agent_server_url: str,
                 consumer_type: str, event_loop: asyncio.AbstractEventLoop,
                 broker_url: str, port: int, presub: bool,
                 manager: "MQTTStreamManager"):
        self.session_id    = session_id
        self.consumer_type = consumer_type
        self.agent_server_url = agent_server_url
        self.presub        = presub

        self.subscriptions: Dict[str, TopicSubscription] = {}

        self.websockets: Set[WebSocket] = set()
        self.ws_lock = asyncio.Lock()

        self._event_loop = event_loop
        self._manager    = manager  

        # Per-session paho client
        paho_client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id=f"stream-service-{session_id}",
            clean_session=True,
        )
        self._purpose_client = PurposeSubscribeClient(
            paho_client,
            log_problems=False,
            purpose_aware=True,
            presub=presub,
        )
        self._paho = paho_client

        self._paho.on_connect    = self._on_connect
        self._paho.on_message    = self._on_message
        self._paho.on_disconnect = self._on_disconnect
        
        self._connected_once = False

        self._paho.connect(broker_url, port, keepalive=60)
        self._paho.loop_start()
        logger.info(f"Session {session_id}: paho client started.")
        
    # ------------------------------------------------------------------
    # Paho callbacks — run on paho's background thread
    # ------------------------------------------------------------------
    def _on_connect(self, client, userdata, flags, rc, properties=None):
        logger.info(f"Session {self.session_id}: MQTT connected (rc={rc}). connected_once={getattr(self, '_connected_once', 'NOT SET YET')}")
        for sub in list(self.subscriptions.values()):
            self._do_subscribe(sub.topic, sub.purpose)
        self._connected_once = True

    def _on_disconnect(self, client, userdata, disconnect_flags, rc, properties=None):
        logger.warning(f"Session {self.session_id}: MQTT disconnected (rc={rc}).")

    def _on_message(self, client, userdata, msg):
        """Bridge incoming message from paho thread to asyncio event loop."""
        topic   = str(msg.topic)
        payload = msg.payload.decode(errors="replace")
        logger.info(f"Session {self.session_id}: message received topic={topic} payload={payload[:80]}")
        data    = {
            "type":      "mqtt_data",
            "topic":     topic,
            "payload":   payload,
            "timestamp": datetime.datetime.now().isoformat(),
        }
        asyncio.run_coroutine_threadsafe(
            self._manager._forward(self, data),
            self._event_loop,
        )

    # ------------------------------------------------------------------
    # Broker operations 
    # ------------------------------------------------------------------
    def _do_subscribe(self, topic: str, purpose: str):
        try:
            self._purpose_client.subscribe_with_purpose(
                topic=topic,
                ap=purpose,
                qos=0,
                presub=self.presub,
            )
            logger.info(f"Session {self.session_id}: broker subscribe topic={topic} purpose={purpose}")
        except Exception as e:
            logger.error(f"Session {self.session_id}: broker subscribe failed for {topic}: {e}")

    def _do_unsubscribe(self, topic: str):
        try:
            self._paho.unsubscribe(topic)
            logger.info(f"Session {self.session_id}: broker unsubscribe topic={topic}")
        except Exception as e:
            logger.error(f"Session {self.session_id}: broker unsubscribe failed for {topic}: {e}")

    def stop(self):
        self._paho.loop_stop()
        self._paho.disconnect()
        logger.info(f"Session {self.session_id}: paho client stopped.")

    # DEBUG
    def _do_subscribe_raw(self, topic: str):
        try:
            self._paho.subscribe(topic, qos=0)
            logger.info(f"Session {self.session_id}: RAW broker subscribe topic={topic}")
        except Exception as e:
            logger.error(f"Session {self.session_id}: RAW broker subscribe failed for {topic}: {e}")


# ---------------------------------------------------------------------------
# Raw subscribe (bypasses PurposeSubscribeClient — diagnostic)
# ---------------------------------------------------------------------------
class RawSubscribeRequest(BaseModel):
    session_id: str
    topic: str
    agent_server_url: Optional[str] = None


# ---------------------------------------------------------------------------
# MQTT Stream Manager
# Owns sessions; each session has its own paho connection.
# ---------------------------------------------------------------------------
class MQTTStreamManager:
    def __init__(self, broker_url: str, port: int, presub: bool):
        self.broker_url = broker_url
        self.port       = port
        self.presub     = presub

        # session_id -> SessionState
        self.sessions: Dict[str, SessionState] = {}
        self.state_lock = asyncio.Lock()
        self._event_loop: Optional[asyncio.AbstractEventLoop] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self, event_loop: asyncio.AbstractEventLoop):
        self._event_loop = event_loop
        logger.info("MQTTStreamManager started.")

    def stop(self):
        for state in list(self.sessions.values()):
            state.stop()
        logger.info("MQTTStreamManager stopped.")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def register_session(self, session_id: str, agent_server_url: str,
                                consumer_type: str = "agent") -> bool:
        logging.debug(f"Registering session: session_id={session_id}, agent_server_url={agent_server_url}, consumer_type={consumer_type}")
        async with self.state_lock:
            if session_id not in self.sessions:
                self.sessions[session_id] = SessionState(
                    session_id=session_id,
                    agent_server_url=agent_server_url,
                    consumer_type=consumer_type,
                    event_loop=self._event_loop,
                    broker_url=self.broker_url,
                    port=self.port,
                    presub=self.presub,
                    manager=self,
                )
                return True
            return False

    async def subscribe(self, session_id: str, topic: str, purpose: str) -> str:
        logging.debug(f"Subscribing session: session_id={session_id}, topic={topic}, purpose={purpose}")
        async with self.state_lock:
            if session_id not in self.sessions:
                return "Session not registered."
            state = self.sessions[session_id]
            if len(state.subscriptions) >= max_topics_per_session:
                return f"Subscription limit reached ({max_topics_per_session} topics max)."
            if topic in state.subscriptions:
                return f"Already subscribed to {topic}."
            state.subscriptions[topic] = TopicSubscription(topic, purpose)

        if state._connected_once:
            state._do_subscribe(topic, purpose)

        logger.info(f"Session {session_id} subscribed to {topic} (purpose={purpose})")
        return f"Subscribed to {topic} with purpose '{purpose}'. Data streaming."

    async def unsubscribe(self, session_id: str, topic: str) -> str:
        async with self.state_lock:
            if session_id not in self.sessions:
                return "Session not registered."

            state = self.sessions[session_id]
            if topic not in state.subscriptions:
                return "No active subscription found."

            del state.subscriptions[topic]

        state._do_unsubscribe(topic)

        logger.info(f"Session {session_id} unsubscribed from {topic}")
        return f"Unsubscribed from {topic}."

    async def cleanup_session(self, session_id: str):
        async with self.state_lock:
            state = self.sessions.pop(session_id, None)
        if not state:
            return
        state.stop()
        logger.info(f"Session {session_id} cleaned up.")

    async def clear_all(self) -> dict:
        async with self.state_lock:
            sessions_copy = list(self.sessions.values())
            num_sessions  = len(self.sessions)
            self.sessions.clear()

        for state in sessions_copy:
            state.stop()
            try:
                async with state.ws_lock:
                    for ws in list(state.websockets):
                        try:
                            await ws.close()
                        except Exception:
                            pass
            except Exception:
                pass

        logger.info(f"Cleared {num_sessions} sessions.")
        return {"result": "cleared", "sessions_removed": num_sessions}

    async def get_all_subscriptions(self) -> dict:
        async with self.state_lock:
            return {
                "sessions": [
                    {
                        "session_id":    state.session_id,
                        "subscriptions": [
                            {"topic": sub.topic, "purpose": sub.purpose}
                            for sub in state.subscriptions.values()
                        ],
                    }
                    for state in self.sessions.values()
                ],
            }

    async def get_session_subscriptions(self, session_id: str) -> Optional[dict]:
        async with self.state_lock:
            state = self.sessions.get(session_id)
            if not state:
                return None
            return {
                "session_id":    state.session_id,
                "subscriptions": [
                    {"topic": sub.topic, "purpose": sub.purpose}
                    for sub in state.subscriptions.values()
                ],
            }
    # DEBUG
    async def subscribe_raw(self, session_id: str, topic: str) -> str:
        async with self.state_lock:
            if session_id not in self.sessions:
                return "Session not registered."
            if len(self.sessions[session_id].subscriptions) >= max_topics_per_session:
                return f"Subscription limit reached."
            if topic in self.sessions[session_id].subscriptions:
                return f"Already subscribed to {topic}."
            self.sessions[session_id].subscriptions[topic] = TopicSubscription(topic, purpose="raw")

        self.sessions[session_id]._do_subscribe_raw(topic)
        return f"RAW subscribed to {topic}. Data streaming."

    # ------------------------------------------------------------------
    # Forwarding
    # ------------------------------------------------------------------
    async def _forward(self, state: SessionState, data: dict):
        if state.consumer_type == "browser":
            await self._forward_ws(state, data)
        else:
            await self._forward_to_agent(state, data)

    async def _forward_to_agent(self, state: SessionState, data: dict):
        url = f"{state.agent_server_url}/agents/{state.session_id}"
        logger.info(f"Forwarding to agent: {url} topic={data['topic']}")
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(url, json={
                    "datapoint": data["payload"],
                    "topic":     data["topic"],
                    "timestamp": data["timestamp"],
                })
                resp.raise_for_status()
                logger.info(f"Forwarded to {url}")
        except Exception as e:
            logger.error(f"Forward failed for session {state.session_id}: {e}")

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
    manager = MQTTStreamManager(
        broker_url=mqtt_broker,
        port=mqtt_port,
        presub=mqtt_presub,
    )
    app.state.manager = manager
    manager.start(asyncio.get_event_loop())
    yield
    manager.stop()

app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
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
        agent_server_url=url,
    )
    result = await manager.subscribe(req.session_id, req.topic, req.purpose)
    return {"session_id": req.session_id, "result": result}


@app.post("/subscribe_browser")
async def subscribe_browser(req: BrowserSubscribeRequest):
    logging.debug(f"subscribe_browser called with session_id={req.session_id}, topic={req.topic}, purpose={req.purpose}")
    manager: MQTTStreamManager = app.state.manager
    await manager.register_session(
        session_id=req.session_id,
        agent_server_url=agent_server_url,
        consumer_type="browser",
    )
    result = await manager.subscribe(req.session_id, req.topic, req.purpose)
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


@app.post("/clear_all")
async def clear_all():
    manager: MQTTStreamManager = app.state.manager
    return await manager.clear_all()

@app.post("/subscribe_raw")
async def subscribe_raw(req: RawSubscribeRequest):
    manager: MQTTStreamManager = app.state.manager
    url = req.agent_server_url or agent_server_url
    await manager.register_session(
        session_id=req.session_id,
        agent_server_url=url,
    )
    result = await manager.subscribe_raw(req.session_id, req.topic)
    return {"session_id": req.session_id, "result": result}

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