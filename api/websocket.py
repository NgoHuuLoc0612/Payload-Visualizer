"""
WebSocket endpoints — real-time payload streaming, live analysis, and broadcast.
"""

from __future__ import annotations

import asyncio
import json
import random
import time
import uuid
from collections import deque
from typing import Any

import orjson
import structlog
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

from core.config import Settings

log = structlog.get_logger(__name__)
router = APIRouter(tags=["WebSocket"])
settings = Settings()

# ── Connection Manager ─────────────────────────────────────────────────────────

class ConnectionManager:
    def __init__(self) -> None:
        self._connections: dict[str, WebSocket] = {}
        self._history: deque[dict] = deque(maxlen=500)

    async def connect(self, ws: WebSocket) -> str:
        await ws.accept()
        cid = str(uuid.uuid4())[:8]
        self._connections[cid] = ws
        log.info("ws_connected", cid=cid, total=len(self._connections))
        return cid

    def disconnect(self, cid: str) -> None:
        self._connections.pop(cid, None)
        log.info("ws_disconnected", cid=cid, total=len(self._connections))

    async def send(self, cid: str, data: dict) -> None:
        ws = self._connections.get(cid)
        if ws and ws.client_state == WebSocketState.CONNECTED:
            try:
                await ws.send_bytes(orjson.dumps(data))
            except Exception:
                self.disconnect(cid)

    async def broadcast(self, data: dict) -> None:
        dead = []
        payload = orjson.dumps(data)
        for cid, ws in self._connections.items():
            try:
                if ws.client_state == WebSocketState.CONNECTED:
                    await ws.send_bytes(payload)
            except Exception:
                dead.append(cid)
        for cid in dead:
            self.disconnect(cid)

    def record(self, event: dict) -> None:
        self._history.append({**event, "ts": time.time()})

    def history(self, n: int = 50) -> list[dict]:
        return list(self._history)[-n:]

    @property
    def connection_count(self) -> int:
        return len(self._connections)


manager = ConnectionManager()


# ── Live Metrics Generator ─────────────────────────────────────────────────────

class MetricsSimulator:
    """Generates realistic streaming payload metrics for real-time visualization."""

    def __init__(self) -> None:
        self._counter = 0
        self._formats = ["json", "xml", "msgpack", "jwt", "graphql", "csv", "protobuf", "binary", "yaml", "har"]
        self._methods = ["POST", "PUT", "PATCH", "GET"]
        self._paths = ["/api/events", "/api/users", "/api/orders", "/graphql", "/ws/data", "/api/metrics"]
        self._latency_base = 45.0
        self._error_rate = 0.04
        self._throughput = 1200.0

    def next_batch(self, size: int = 20) -> list[dict]:
        now = time.time()
        batch = []
        for _ in range(size):
            self._counter += 1
            fmt = random.choice(self._formats)
            sz = int(random.lognormvariate(7, 1.5))
            latency = max(1, random.gauss(self._latency_base, 18))
            is_error = random.random() < self._error_rate

            # Drift simulation
            self._latency_base = max(10, self._latency_base + random.gauss(0, 0.5))
            self._throughput = max(100, self._throughput + random.gauss(0, 30))
            self._error_rate = max(0, min(0.25, self._error_rate + random.gauss(0, 0.002)))

            batch.append({
                "id": self._counter,
                "ts": now,
                "format": fmt,
                "method": random.choice(self._methods),
                "path": random.choice(self._paths),
                "size_bytes": sz,
                "latency_ms": round(latency, 2),
                "status": random.choice([500, 503, 400]) if is_error else random.choice([200, 201, 204]),
                "error": is_error,
                "throughput_rps": round(self._throughput, 1),
                "parse_time_ms": round(latency * random.uniform(0.1, 0.4), 3),
                "fields_count": random.randint(3, 80),
                "anomaly_count": random.randint(0, 3) if random.random() < 0.15 else 0,
            })
        return batch

    def system_snapshot(self) -> dict:
        return {
            "type": "system_snapshot",
            "ts": time.time(),
            "connections": manager.connection_count,
            "total_processed": self._counter,
            "avg_latency_ms": round(self._latency_base, 2),
            "throughput_rps": round(self._throughput, 1),
            "error_rate_pct": round(self._error_rate * 100, 2),
            "format_distribution": _random_format_dist(self._formats),
            "memory_mb": round(random.uniform(120, 340), 1),
            "cpu_pct": round(random.uniform(5, 65), 1),
        }


_simulator = MetricsSimulator()


def _random_format_dist(formats: list[str]) -> dict[str, int]:
    weights = [random.randint(5, 100) for _ in formats]
    total = sum(weights)
    return {fmt: round(w / total * 100, 1) for fmt, w in zip(formats, weights)}


# ── WebSocket Handlers ─────────────────────────────────────────────────────────

@router.websocket("/ws/stream")
async def ws_stream(websocket: WebSocket):
    """
    Main real-time stream endpoint.
    Client sends JSON commands; server streams metric batches + system snapshots.

    Commands:
      {"cmd": "subscribe", "interval_ms": 250, "batch_size": 20}
      {"cmd": "parse", "payload": "...", "format": "json"}
      {"cmd": "history", "n": 50}
      {"cmd": "ping"}
    """
    cid = await manager.connect(websocket)

    await manager.send(cid, {
        "type": "welcome",
        "cid": cid,
        "ts": time.time(),
        "message": "Payload Visualizer stream connected",
    })

    stream_task: asyncio.Task | None = None
    interval_ms = settings.stream_interval_ms
    batch_size = settings.stream_batch_size

    async def _stream_loop():
        snapshot_every = 8
        tick = 0
        while True:
            await asyncio.sleep(interval_ms / 1000)
            batch = _simulator.next_batch(batch_size)
            manager.record({"type": "batch", "count": len(batch)})

            await manager.send(cid, {
                "type": "metrics_batch",
                "ts": time.time(),
                "batch": batch,
            })

            tick += 1
            if tick % snapshot_every == 0:
                await manager.send(cid, _simulator.system_snapshot())

    try:
        stream_task = asyncio.create_task(_stream_loop())

        while True:
            try:
                raw = await asyncio.wait_for(websocket.receive_text(), timeout=settings.ws_heartbeat_interval)
                cmd = json.loads(raw)
            except asyncio.TimeoutError:
                await manager.send(cid, {"type": "heartbeat", "ts": time.time()})
                continue
            except WebSocketDisconnect:
                break

            c = cmd.get("cmd", "")

            if c == "ping":
                await manager.send(cid, {"type": "pong", "ts": time.time()})

            elif c == "subscribe":
                interval_ms = max(50, int(cmd.get("interval_ms", interval_ms)))
                batch_size = max(1, min(200, int(cmd.get("batch_size", batch_size))))
                if stream_task:
                    stream_task.cancel()
                stream_task = asyncio.create_task(_stream_loop())
                await manager.send(cid, {"type": "subscribed", "interval_ms": interval_ms, "batch_size": batch_size})

            elif c == "parse":
                payload_text = cmd.get("payload", "")
                fmt = cmd.get("format", "auto")
                result = await _ws_parse(websocket.app, payload_text.encode(), fmt)
                await manager.send(cid, {"type": "parse_result", **result})

            elif c == "history":
                n = int(cmd.get("n", 50))
                await manager.send(cid, {"type": "history", "events": manager.history(n)})

            elif c == "pause":
                if stream_task:
                    stream_task.cancel()
                    stream_task = None
                await manager.send(cid, {"type": "paused"})

            elif c == "resume":
                if not stream_task or stream_task.done():
                    stream_task = asyncio.create_task(_stream_loop())
                await manager.send(cid, {"type": "resumed"})

    except WebSocketDisconnect:
        pass
    except Exception as exc:
        log.error("ws_error", cid=cid, error=str(exc))
    finally:
        if stream_task:
            stream_task.cancel()
        manager.disconnect(cid)


async def _ws_parse(app: Any, raw: bytes, fmt: str) -> dict:
    registry = app.state.registry
    if fmt == "auto":
        proc = registry.detect(raw)
    else:
        proc = registry.get(fmt)
    if proc is None:
        return {"error": f"Unknown format: {fmt}"}
    try:
        result = proc.parse(raw)
        return orjson.loads(result.model_dump_json_bytes())
    except Exception as exc:
        return {"error": str(exc)}


@router.websocket("/ws/broadcast")
async def ws_broadcast(websocket: WebSocket):
    """Receive payloads and broadcast parsed results to all clients."""
    cid = await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            cmd = json.loads(data)
            payload = cmd.get("payload", "")
            fmt = cmd.get("format", "auto")
            result = await _ws_parse(websocket.app, payload.encode(), fmt)
            await manager.broadcast({"type": "broadcast_result", "from": cid, **result})
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(cid)
