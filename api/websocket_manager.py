"""
VenueFlow – WebSocket Connection Manager (Phase 3 · Module A)
=============================================================
Manages room-based WebSocket connections with three room types:

  fan:{seat_id}   — personalised per-seat alerts for individual fans
  zone:{zone}     — area-wide broadcasts (signage, zone dashboards)
  ops             — global operations channel for staff / dashboards

Design decisions:
  • All mutable state is protected by an asyncio.Lock so coroutines sharing
    the event loop never race on the connections dict.
  • broadcast_* methods silently drop dead sockets and clean up eagerly.
  • send_personal / broadcast never raise; errors are logged and swallowed so
    one bad client cannot crash the bridge loop.
"""

import asyncio
import json
import logging
from collections import defaultdict
from datetime import datetime
from typing import Any

from fastapi import WebSocket

log = logging.getLogger("vf.ws")


class ConnectionManager:
    """
    Thread-safe (asyncio-safe) WebSocket room manager.

    Rooms:
        fan:{seat_id}    — individual fan socket (usually one connection)
        zone:{zone}      — all sockets for a venue zone
        ops              — all ops-dashboard sockets
    """

    def __init__(self) -> None:
        # Stores: room_key → set of WebSocket objects
        self._rooms: dict[str, set[WebSocket]] = defaultdict(set)
        self._lock = asyncio.Lock()

    # ── helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _fan_room(seat_id: str) -> str:
        return f"fan:{seat_id}"

    @staticmethod
    def _zone_room(zone: str) -> str:
        return f"zone:{zone}"

    OPS_ROOM = "ops"

    # ── lifecycle ─────────────────────────────────────────────────────────────

    async def connect_fan(self, ws: WebSocket, seat_id: str) -> None:
        """Accept a fan WebSocket and register it in the fan room."""
        await ws.accept()
        async with self._lock:
            self._rooms[self._fan_room(seat_id)].add(ws)
        log.info("WS CONNECT  fan:%s  (total rooms: %d)", seat_id, len(self._rooms))

    async def connect_zone(self, ws: WebSocket, zone: str) -> None:
        """Accept a zone WebSocket and register it in the zone room."""
        await ws.accept()
        async with self._lock:
            self._rooms[self._zone_room(zone)].add(ws)
        log.info("WS CONNECT  zone:%s  (total rooms: %d)", zone, len(self._rooms))

    async def connect_ops(self, ws: WebSocket) -> None:
        """Accept an ops WebSocket and register it in the ops room."""
        await ws.accept()
        async with self._lock:
            self._rooms[self.OPS_ROOM].add(ws)
        log.info("WS CONNECT  ops  (ops total: %d)", len(self._rooms[self.OPS_ROOM]))

    async def disconnect(self, ws: WebSocket) -> None:
        """Remove a WebSocket from every room it is registered in."""
        async with self._lock:
            empty_rooms: list[str] = []
            for room, sockets in self._rooms.items():
                sockets.discard(ws)
                if not sockets:
                    empty_rooms.append(room)
            for room in empty_rooms:
                del self._rooms[room]
        log.debug("WS DISCONNECT – cleaned %d empty room(s)", len(empty_rooms))

    # ── sending ───────────────────────────────────────────────────────────────

    async def _safe_send(self, ws: WebSocket, payload: str) -> bool:
        """
        Try to send text to a single socket.
        Returns True on success, False on any error (caller handles eviction).
        """
        try:
            await ws.send_text(payload)
            return True
        except Exception as exc:
            log.debug("WS send error (will evict): %s", exc)
            return False

    async def _broadcast_room(self, room_key: str, payload: str) -> None:
        """Send payload to all sockets in a room; evict dead ones."""
        async with self._lock:
            sockets = set(self._rooms.get(room_key, set()))

        dead: list[WebSocket] = []
        await asyncio.gather(
            *[self._send_and_flag(ws, payload, dead) for ws in sockets],
            return_exceptions=True,
        )

        if dead:
            async with self._lock:
                for ws in dead:
                    self._rooms[room_key].discard(ws)
                if not self._rooms.get(room_key):
                    self._rooms.pop(room_key, None)

    async def _send_and_flag(
        self, ws: WebSocket, payload: str, dead_list: list[WebSocket]
    ) -> None:
        ok = await self._safe_send(ws, payload)
        if not ok:
            dead_list.append(ws)

    # ── public broadcast API ──────────────────────────────────────────────────

    async def send_to_fan(self, seat_id: str, data: Any) -> None:
        """Push JSON-serialisable data to a specific fan's seat room."""
        payload = _serialise(data)
        await self._broadcast_room(self._fan_room(seat_id), payload)

    async def broadcast_zone(self, zone: str, data: Any) -> None:
        """Push JSON-serialisable data to all sockets in a zone room."""
        payload = _serialise(data)
        await self._broadcast_room(self._zone_room(zone), payload)

    async def broadcast_ops(self, data: Any) -> None:
        """Push JSON-serialisable data to the global ops channel."""
        payload = _serialise(data)
        await self._broadcast_room(self.OPS_ROOM, payload)

    # ── stats ─────────────────────────────────────────────────────────────────

    async def stats(self) -> dict:
        """Return a snapshot of active room sizes (for /health or /ws/debug)."""
        async with self._lock:
            return {
                "rooms": {k: len(v) for k, v in self._rooms.items()},
                "total_connections": sum(len(v) for v in self._rooms.values()),
                "timestamp": datetime.utcnow().isoformat(),
            }


# ── module-level singleton ────────────────────────────────────────────────────

manager = ConnectionManager()


# ── serialisation helper ──────────────────────────────────────────────────────

def _serialise(data: Any) -> str:
    """Convert dict / Pydantic model / primitive to a JSON string."""
    if isinstance(data, str):
        return data
    if hasattr(data, "model_dump_json"):          # Pydantic v2
        return data.model_dump_json()
    if hasattr(data, "json"):                     # Pydantic v1 compat
        return data.json()
    return json.dumps(data, default=str)
