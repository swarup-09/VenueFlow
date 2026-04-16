"""
VenueFlow – Async Database Layer
asyncpg connection pool + typed CRUD helpers for the Digital Twin.
"""

import os
import asyncpg
from datetime import datetime
from typing import Optional
import json

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://vf_user:vf_secret@localhost:5432/venueflow",
)

# Strip asyncpg prefix if SQLAlchemy URL passed in
_PG_URL = DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")

_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            dsn=_PG_URL,
            min_size=2,
            max_size=10,
            command_timeout=10,
        )
    return _pool


async def close_pool():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


# ─── Raw insert helpers (called by consumer) ─────────────────────────────────

async def insert_crowd_reading(row: dict):
    pool = await get_pool()
    await pool.execute(
        """
        INSERT INTO crowd_readings
            (time, sensor_id, zone, density, headcount, flow_rate, direction)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        """,
        row["timestamp"], row["sensor_id"], row["zone"],
        row["density"], row["headcount"], row["flow_rate"], row["direction"],
    )


async def insert_queue_reading(row: dict):
    pool = await get_pool()
    await pool.execute(
        """
        INSERT INTO queue_readings
            (time, sensor_id, stand_id, stand_name, zone,
             queue_length, wait_minutes, capacity_pct, is_open)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """,
        row["timestamp"], row["sensor_id"], row["stand_id"],
        row["stand_name"], row["zone"], row["queue_length"],
        row["wait_minutes"], row["capacity_pct"], row["is_open"],
    )


async def upsert_digital_twin_zone(zone: str, data: dict):
    """Update the live zone state in the Digital Twin."""
    pool = await get_pool()
    await pool.execute(
        """
        INSERT INTO digital_twin_zones
            (zone, last_updated, density, headcount, flow_rate,
             direction, congestion_level)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (zone) DO UPDATE SET
            last_updated    = EXCLUDED.last_updated,
            density         = EXCLUDED.density,
            headcount       = EXCLUDED.headcount,
            flow_rate       = EXCLUDED.flow_rate,
            direction       = EXCLUDED.direction,
            congestion_level = EXCLUDED.congestion_level
        """,
        zone, data["timestamp"], data["density"], data["headcount"],
        data["flow_rate"], data["direction"], data.get("congestion_level", "unknown"),
    )


async def upsert_digital_twin_stand(stand_id: str, data: dict):
    """Update the live stand state in the Digital Twin."""
    pool = await get_pool()
    await pool.execute(
        """
        INSERT INTO digital_twin_stands
            (stand_id, stand_name, zone, last_updated,
             queue_length, wait_minutes, capacity_pct, is_open)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (stand_id) DO UPDATE SET
            last_updated    = EXCLUDED.last_updated,
            queue_length    = EXCLUDED.queue_length,
            wait_minutes    = EXCLUDED.wait_minutes,
            capacity_pct    = EXCLUDED.capacity_pct,
            is_open         = EXCLUDED.is_open
        """,
        stand_id, data["stand_name"], data["zone"], data["timestamp"],
        data["queue_length"], data["wait_minutes"], data["capacity_pct"],
        data["is_open"],
    )


async def upsert_wait_prediction(stand_id: str, pred: dict):
    pool = await get_pool()
    await pool.execute(
        """
        UPDATE digital_twin_stands SET
            predicted_wait_5min     = $2,
            predicted_wait_10min    = $3,
            halftime_spike_expected = $4
        WHERE stand_id = $1
        """,
        stand_id, pred["predicted_wait_5min"],
        pred["predicted_wait_10min"], pred["halftime_spike_expected"],
    )
    await pool.execute(
        """
        INSERT INTO wait_predictions
            (time, stand_id, stand_name, predicted_wait_5min, predicted_wait_10min,
             arrival_rate_lambda, service_rate_mu, server_count,
             utilization_rho, halftime_spike_expected)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        """,
        datetime.utcnow(), stand_id, pred["stand_name"],
        pred["predicted_wait_5min"], pred["predicted_wait_10min"],
        pred["lambda_"], pred["mu"], pred["c"],
        pred["rho"], pred["halftime_spike_expected"],
    )


async def upsert_flow_prediction(zone: str, pred: dict):
    pool = await get_pool()
    await pool.execute(
        """
        UPDATE digital_twin_zones SET
            congestion_level = $2
        WHERE zone = $1
        """,
        zone, pred["congestion_risk"],
    )
    await pool.execute(
        """
        INSERT INTO flow_predictions
            (time, zone, predicted_density, recommended_route,
             congestion_risk, avoid_zones)
        VALUES ($1,$2,$3,$4,$5,$6)
        """,
        datetime.utcnow(), zone,
        pred["predicted_density"], pred.get("recommended_route"),
        pred["congestion_risk"], pred.get("avoid_zones", []),
    )


async def insert_alert(severity: str, category: str, message: str,
                        zone: str = None, stand_id: str = None):
    pool = await get_pool()
    await pool.execute(
        """
        INSERT INTO alerts (severity, category, zone, stand_id, message)
        VALUES ($1, $2, $3, $4, $5)
        """,
        severity, category, zone, stand_id, message,
    )


# ─── Query helpers (called by API) ────────────────────────────────────────────

async def get_all_zones() -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch("SELECT * FROM digital_twin_zones ORDER BY zone")
    return [dict(r) for r in rows]


async def get_zone(zone: str) -> Optional[dict]:
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT * FROM digital_twin_zones WHERE zone = $1", zone
    )
    return dict(row) if row else None


async def get_all_stands() -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT * FROM digital_twin_stands ORDER BY wait_minutes ASC"
    )
    return [dict(r) for r in rows]


async def get_stand(stand_id: str) -> Optional[dict]:
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT * FROM digital_twin_stands WHERE stand_id = $1",
        stand_id.upper(),
    )
    return dict(row) if row else None


async def get_active_alerts(limit: int = 20) -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT * FROM alerts
        WHERE resolved = FALSE
        ORDER BY time DESC
        LIMIT $1
        """,
        limit,
    )
    return [dict(r) for r in rows]


async def get_crowd_history(zone: str, minutes: int = 30) -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT bucket, avg_density, max_headcount, avg_flow_rate
        FROM crowd_1min
        WHERE zone = $1 AND bucket > NOW() - ($2 || ' minutes')::interval
        ORDER BY bucket ASC
        """,
        zone, str(minutes),
    )
    return [dict(r) for r in rows]


async def get_queue_history(stand_id: str, minutes: int = 30) -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT bucket, avg_queue_length, avg_wait_minutes, avg_capacity_pct
        FROM queue_1min
        WHERE stand_id = $1 AND bucket > NOW() - ($2 || ' minutes')::interval
        ORDER BY bucket ASC
        """,
        stand_id.upper(), str(minutes),
    )
    return [dict(r) for r in rows]


# ─── Phase 3: Fan Orders ──────────────────────────────────────────────────────

async def insert_fan_order(order: dict) -> None:
    """Persist a new fan order to the fan_orders hypertable."""
    pool = await get_pool()
    await pool.execute(
        """
        INSERT INTO fan_orders (
            placed_at, order_id, seat_id, stand_id, stand_name, zone,
            items, status, queue_wait_minutes, walk_minutes,
            delivery_eta_minutes, delivery_eta_timestamp,
            total_price, special_instructions
        ) VALUES (
            $1, $2, $3, $4, $5, $6,
            $7::jsonb, $8, $9, $10,
            $11, $12,
            $13, $14
        )
        """,
        order.get("placed_at") or __import__("datetime").datetime.utcnow(),
        __import__("uuid").UUID(order["order_id"]),
        order["seat_id"],
        order["stand_id"],
        order["stand_name"],
        order.get("zone"),
        order["items"] if isinstance(order["items"], str)
            else __import__("json").dumps(order["items"]),
        order.get("status", "placed"),
        order.get("queue_wait_minutes"),
        order.get("walk_minutes"),
        order.get("delivery_eta_minutes"),
        __import__("datetime").datetime.fromisoformat(order["delivery_eta_timestamp"])
            if isinstance(order.get("delivery_eta_timestamp"), str)
            else order.get("delivery_eta_timestamp"),
        order.get("total_price"),
        order.get("special_instructions"),
    )


async def get_fan_orders(
    seat_id: str,
    limit: int = 20,
    status_filter: Optional[str] = None,
) -> list[dict]:
    """Return order history for a seat, newest first."""
    pool = await get_pool()
    if status_filter:
        rows = await pool.fetch(
            """
            SELECT order_id, seat_id, stand_id, stand_name, zone,
                   items, status, queue_wait_minutes, walk_minutes,
                   delivery_eta_minutes, delivery_eta_timestamp,
                   total_price, special_instructions, placed_at, updated_at
            FROM fan_orders
            WHERE seat_id = $1 AND status = $2
            ORDER BY placed_at DESC
            LIMIT $3
            """,
            seat_id, status_filter, limit,
        )
    else:
        rows = await pool.fetch(
            """
            SELECT order_id, seat_id, stand_id, stand_name, zone,
                   items, status, queue_wait_minutes, walk_minutes,
                   delivery_eta_minutes, delivery_eta_timestamp,
                   total_price, special_instructions, placed_at, updated_at
            FROM fan_orders
            WHERE seat_id = $1
            ORDER BY placed_at DESC
            LIMIT $2
            """,
            seat_id, limit,
        )

    result = []
    for r in rows:
        row = dict(r)
        # Ensure order_id is a plain string for JSON serialisation
        if row.get("order_id"):
            row["order_id"] = str(row["order_id"])
        # Convert timestamps to ISO strings
        for ts_col in ("placed_at", "updated_at", "delivery_eta_timestamp"):
            if row.get(ts_col) and hasattr(row[ts_col], "isoformat"):
                row[ts_col] = row[ts_col].isoformat()
        result.append(row)
    return result
