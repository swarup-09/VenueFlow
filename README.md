# VenueFlow — Smart Venue Intelligence Platform

Real-time crowd management and fan experience system for large-scale sporting venues.
All sensors are virtual (no hardware required). The full stack runs locally with a single Docker command.

---

## Quick Start

```bash
cd venueflow
docker compose up --build

# API docs:    http://localhost:8000/docs
# Kafka UI:    http://localhost:8080
# TimescaleDB: localhost:5432  (user: vf_user  pass: vf_secret  db: venueflow)
```

---

## Project Structure

```
venueflow/
├── api/
│   ├── main.py                  # FastAPI app — HTTP + WebSocket endpoints (Phase 1-3)
│   ├── fan_routes.py            # Phase 3 · Module B: Fan / Attendee API endpoints
│   ├── ops_routes.py            # Phase 3 · Module C: Ops Command Layer (heatmap, exit-plan, alerts)
│   ├── websocket_manager.py     # Phase 3: room-based WS manager (fan / zone / ops)
│   └── kafka_bridge.py          # Phase 3: Kafka → WebSocket bridge (background task)
├── sensors/
│   ├── crowd_sensors.py         # Virtual footfall / pressure-plate sensors (12 zones × 2)
│   ├── queue_sensors.py         # Virtual concession queue sensors (8 stands)
│   ├── ble_cctv_sensors.py      # Virtual BLE beacons (15) + CCTV cameras (12)
│   └── orchestrator.py          # Combines all sensors → VenueSnapshot
├── intelligence/
│   ├── wait_estimator.py        # M/M/c Erlang C queuing engine + halftime spike detector
│   └── flow_predictor.py        # Dijkstra crowd-aware routing + ETS density forecasting
├── consumer/
│   └── service.py               # Kafka consumer — persists data, runs intelligence engines
├── db/
│   ├── init.sql                 # TimescaleDB schema, hypertables, continuous aggregates
│   └── database.py              # asyncpg pool + typed CRUD helpers for Digital Twin
├── models/
│   └── schemas.py               # Pydantic v2 models for all sensor and snapshot types
├── docker/
│   ├── Dockerfile.api           # Container for FastAPI service
│   └── Dockerfile.consumer      # Container for Kafka consumer service
├── docker-compose.yml           # Full stack: Kafka · TimescaleDB · API · Consumer · UI
└── requirements.txt
```

---

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    EXPERIENCE LAYER                      │
│    Attendee App · Ops Dashboard · Smart Displays         │
└───────────────────────┬──────────────────────────────────┘
                        │ HTTP / SSE / WebSocket
┌───────────────────────▼──────────────────────────────────┐
│              FastAPI  :8000  (api/main.py)               │
│   /sensors/*   /twin/*   /intelligence/*   /alerts       │
└─────────┬─────────────────────────────┬──────────────────┘
          │ produce                     │ query
          ▼                             ▼
┌──────────────────┐       ┌────────────────────────────┐
│  Apache Kafka    │       │  TimescaleDB  :5432         │
│  :9092  KRaft    │       │  Digital Twin · Hypertables │
│                  │       │  Continuous Aggregates      │
│  vf_crowd_raw    │       └──────────────┬─────────────┘
│  vf_queues_raw   │                      │ write
│  vf_ble_raw      │       ┌──────────────▼─────────────┐
│  vf_cctv_raw     │◄──────┤  Consumer (consumer/service)│
│  vf_intel_out    │       │  Wait Estimator  (M/M/c)   │
│  vf_alerts       │──────►│  Flow Predictor  (Dijkstra) │
└──────────────────┘       └────────────────────────────┘
          ▲
┌─────────┴────────────────────────────────────────────────┐
│                     SENSING LAYER                        │
│   Crowd Sensors · Queue Sensors · BLE Beacons · CCTV     │
└──────────────────────────────────────────────────────────┘
```

---

## Phase 1 — Sensing Layer

### Virtual Sensor Types

| Type | Count | Output fields |
|---|---|---|
| Crowd sensors | 12 zones × 2 | density (0–1), headcount, flow_rate, direction |
| Queue sensors | 8 stands | queue_length, wait_minutes, capacity_pct |
| BLE beacons | 15 locations | active_devices, rssi_avg (dBm), dwell_time_avg |
| CCTV cameras | 12 cameras | headcount_estimate, motion_intensity, anomaly flags |

Sensors use sinusoidal halftime event cycles, Gaussian noise, and Little's Law queuing
models to produce realistic time-varying data with no hardware or external feeds.

### Kafka Topics

| Topic | Partitions | Publisher | Consumer |
|---|---|---|---|
| `vf_crowd_raw` | 4 | FastAPI on each `/sensors/*` call | Consumer service |
| `vf_queues_raw` | 4 | FastAPI on each `/sensors/*` call | Consumer service |
| `vf_ble_raw` | 2 | FastAPI | Consumer service |
| `vf_cctv_raw` | 2 | FastAPI | Consumer service |
| `vf_intelligence_out` | 2 | Consumer (post-ML) | Dashboards / downstream |
| `vf_alerts` | 1 | Consumer (on threshold breach) | Ops / notification service |

---

## Phase 2 — Intelligence Layer

### Wait Estimator — M/M/c Erlang C

Uses classical queuing theory to predict concession stand wait times before they happen.

```
λ  = arrival rate (customers/min)   estimated from rolling queue-length delta
μ  = service rate per till          configured per stand (1.2 – 2.3 ppl/min)
c  = open tills                     configured per stand (2 – 4)
ρ  = λ / (c·μ)                      utilisation (must be < 1 for stable queue)

P(wait) = Erlang C(λ, μ, c)         probability a customer must queue at all
Wq      = P(wait) / (c·μ·(1−ρ))    mean wait in queue (minutes)
```

Halftime spike detection flags when 3+ consecutive queue readings show >20% growth,
giving ops an 8-minute warning to open a reserve till or push a mobile-order nudge.

### Flow Predictor — Dijkstra on a Density-Weighted Graph

Computes crowd-aware walking routes across the 12-node venue graph.

```
Edge weight:  w_eff = w_base × (1 + 4 × density²)
Hard block:   zones with density ≥ 90% are removed from the graph entirely
Forecast:     D̂_{t+1} = 0.3 × D_t + 0.7 × D̂_t   (ETS, α = 0.3)
```

Example: Gate-East at 92% density → router automatically reroutes
`Gate-North → Lower-Stands` through the VIP-Lounge bypass (100s) instead of the
direct congested path.

### Digital Twin — TimescaleDB

One live row per zone and per stand, updated by the consumer via `ON CONFLICT DO UPDATE`.
The API queries this table to answer "How long is the line at Stand C4?" in a single
indexed lookup rather than running a fresh simulation.

**Hypertables (raw time-series):**
`crowd_readings` · `queue_readings` · `flow_predictions` · `wait_predictions` · `alerts`

**Continuous aggregates** (auto-refreshed every 60 s by TimescaleDB):
`crowd_1min` · `queue_1min` — 1-minute bucketed averages used by history endpoints.

---

## API Reference

### Info

| Method | Path | Description |
|---|---|---|
| GET | `/` | Service info and endpoint map |
| GET | `/health` | Kafka ready flag, TimescaleDB connectivity, unresolved critical alert count |

### Phase 1 — Sensors

| Method | Path | Description |
|---|---|---|
| GET | `/sensors/snapshot` | Full VenueSnapshot (all 4 sensor types) + publishes to Kafka |
| GET | `/sensors/crowd` | All crowd sensor readings |
| GET | `/sensors/crowd/{zone}` | Crowd readings filtered to one zone |
| GET | `/sensors/queues` | All concession queue readings |
| GET | `/sensors/queues/shortest?top_n=3` | Stands sorted by shortest current wait |
| GET | `/sensors/queues/{stand_id}` | Single stand reading |
| GET | `/sensors/ble` | All BLE beacon readings |
| GET | `/sensors/cctv` | All CCTV camera readings |
| GET | `/sensors/cctv/anomalies` | Only cameras with active anomaly detections |
| GET | `/sensors/stream?interval=3` | SSE live stream — VenueSnapshot every N seconds |

### Phase 2 — Digital Twin

| Method | Path | Description |
|---|---|---|
| GET | `/twin/zones` | All zones from the live Digital Twin |
| GET | `/twin/zones/{zone}` | Single zone state (density, congestion, BLE, CCTV) |
| GET | `/twin/stands` | All stands from Digital Twin, sorted by wait time |
| GET | `/twin/stands/{stand_id}` | Single stand with ML predictions from consumer |
| GET | `/twin/alerts` | Active (unresolved) alerts written by the consumer |

### Phase 2 — History

| Method | Path | Description |
|---|---|---|
| GET | `/history/crowd/{zone}?minutes=30` | 1-min bucketed crowd density history |
| GET | `/history/queues/{stand_id}?minutes=30` | 1-min bucketed queue depth history |

### Phase 2 — Intelligence (on-demand, no DB required)

| Method | Path | Description |
|---|---|---|
| GET | `/intelligence/wait` | M/M/c predictions — ρ, Wq, 5-min/10-min forecasts, spike flag |
| GET | `/intelligence/flow` | ETS density forecast + congestion risk per zone |
| GET | `/intelligence/route?from_zone=X&to_zone=Y` | Crowd-aware Dijkstra route |
| GET | `/alerts` | Combined sensor + intelligence alerts |

### Phase 3 · Module B — Fan API

| Method | Path | Description |
|---|---|---|
| GET | `/fan/{seat_id}/nearby` | Top-N stands by wait time with crowd-aware walking routes |
| POST | `/fan/{seat_id}/order` | Place food order → M/M/c ETA → Kafka `vf_orders` → TimescaleDB |
| GET | `/fan/{seat_id}/orders` | Order history + status filter for a seat |

### Phase 3 · Module C — Ops Command

| Method | Path | Description |
|---|---|---|
| GET | `/ops/heatmap` | Nested thermal state of every zone + stand (density, wait, anomalies) |
| GET | `/ops/exit-plan` | Staggered post-match departure windows with auto-rerouting around blocked gates |
| POST | `/ops/alerts/resolve/{alert_id}` | Mark an alert UUID as resolved in TimescaleDB |

---

## Valid Zones and Stand IDs

**Zones**
```
Gate-North    Gate-South    Gate-East    Gate-West
Concourse-A   Concourse-B   Concourse-C
Lower-Stands  Upper-Stands  VIP-Lounge
Parking-P1    Parking-P2
```

**Stands**
```
C4  Hot Dogs & Snacks    (Concourse-A)
B2  Beer & Beverages     (Concourse-B)
A7  Pizza Corner         (Concourse-A)
B5  Burgers & Fries      (Concourse-B)
C1  Coffee & Desserts    (Concourse-C)
A3  Nachos & Wraps       (Concourse-A)
D2  VIP Bar              (VIP-Lounge)
B8  South Grill          (Concourse-B)
```

---

## Usage Examples

```bash
# Full snapshot — also publishes all readings to Kafka
curl http://localhost:8000/sensors/snapshot | jq .

# Live SSE stream — one VenueSnapshot every 3 seconds
curl -N http://localhost:8000/sensors/stream

# Digital Twin: how long is the line at Stand C4?
curl http://localhost:8000/twin/stands/C4 \
  | jq '{wait_minutes, predicted_wait_5min, halftime_spike_expected}'

# Crowd-aware route from parking to seats
curl "http://localhost:8000/intelligence/route?from_zone=Parking-P1&to_zone=Lower-Stands" \
  | jq '{path, estimated_minutes, avoided_zones}'

# Which stands are about to spike at halftime?
curl http://localhost:8000/intelligence/wait \
  | jq '[.[] | select(.halftime_spike_expected==true) | {stand_id, predicted_wait_10min}]'

# 30-minute crowd density history for Concourse-A
curl "http://localhost:8000/history/crowd/Concourse-A?minutes=30" | jq .

# Active alerts
curl http://localhost:8000/alerts | jq .
```

---

## Docker Services

| Container | Image | Port | Role |
|---|---|---|---|
| `vf-kafka` | apache/kafka:4.0.0 | 9092 | Kafka broker (KRaft, no Zookeeper) |
| `vf-kafka-init` | apache/kafka:4.0.0 | — | One-shot topic creation, exits on completion |
| `vf-kafka-ui` | provectuslabs/kafka-ui | 8080 | Visual broker and message monitoring |
| `vf-timescaledb` | timescale/timescaledb:latest-pg16 | 5432 | Digital Twin + time-series storage |
| `vf-api` | (local build) | 8000 | FastAPI — all sensor and intelligence endpoints |
| `vf-consumer` | (local build) | — | Kafka consumer + intelligence runner |

---

## Dependencies

```
fastapi>=0.110.0          HTTP framework
uvicorn[standard]>=0.29.0 ASGI server
pydantic>=2.0.0            Data validation
confluent-kafka>=2.4.0     Kafka producer / consumer
asyncpg>=0.29.0            Async PostgreSQL driver
```

Intelligence engines (`wait_estimator.py`, `flow_predictor.py`) use Python stdlib only —
`math`, `heapq`, `dataclasses`. No scikit-learn or heavy ML frameworks required.

---

## What's Next — Phase 3 (Experience Layer)

| Feature | Endpoint | Description |
|---|---|---|
| **WebSocket push** | `WS /ws/fan/{seat_id}` | Personalised real-time alerts per fan ✅ |
| **Zone broadcast** | `WS /ws/zone/{zone}` | Push to all fans in a zone (signage) ✅ |
| **Ops channel** | `WS /ws/ops` | Full venue feed for staff dashboards ✅ |
| **WS debug** | `GET /ws/debug` | Live room and connection counts ✅ |
| **Fan nearby** | `GET /fan/{seat_id}/nearby` | Top-3 stands by wait + crowd-aware walking route ✅ |
| **Mobile order** | `POST /fan/{seat_id}/order` | Place order → M/M/c ETA → Kafka `vf_orders` → DB ✅ |
| **Order history** | `GET /fan/{seat_id}/orders` | Order history + status from TimescaleDB ✅ |
| **Ops heatmap** | `GET /ops/heatmap` | Nested thermal view — zone density + stand queues + anomalies ✅ |
| **Exit planner** | `GET /ops/exit-plan` | Staggered post-match exit with hard-block rerouting ✅ |
| **Alert resolve** | `POST /ops/alerts/resolve/{id}` | Mark a critical alert resolved by ops staff ✅ |

### Phase 3 — WebSocket Message Types

All WebSocket messages are JSON with a `_type` field for client-side routing:

| `_type` | Source topic | Routing |
|---|---|---|
| `flow_intelligence` | `vf_intelligence_out` | `zone:{zone}` + `ops` |
| `wait_intelligence` | `vf_intelligence_out` | `ops` only |
| `alert` | `vf_alerts` | `fan:{seat_id}` or `zone:{zone}` or `ops` (priority order) |

### Phase 3 · Module B — Fan API Detail

#### `GET /fan/{seat_id}/nearby`
Returns top-N concession stands sorted by current `wait_minutes` with a crowd-aware walking route and `total_eta_minutes` (queue wait + walk) per stand.

#### `POST /fan/{seat_id}/order`
```json
{
  "stand_id": "C4",
  "items": [{"name": "Nachos", "quantity": 2, "price": 8.50}],
  "special_instructions": "Extra salsa please"
}
```
Response includes `delivery_eta_minutes` = M/M/c queue wait + Flow Predictor walk time + packaging overhead.  
Order is published to the `vf_orders` Kafka topic and persisted to the `fan_orders` TimescaleDB hypertable.

#### `GET /fan/{seat_id}/orders?status=placed`
Returns order history with optional `status` filter: `placed | preparing | delivered | cancelled`.

---

### Phase 3 · Module C — Ops Command Detail

#### `GET /ops/heatmap`
Joins `digital_twin_zones` ⋈ `digital_twin_stands` and returns a fully nested JSON thermal map of the stadium. Every zone carries live `density`, `headcount`, `flow_rate`, `congestion_level`, and `anomaly_active` flag. Each zone embeds its list of concession stands with queue depth, wait times, and M/M/c predictions.

Top-level envelope includes `critical_zones` — any zone currently at ≥ 90 % density.

#### `GET /ops/exit-plan`
Generates a prioritised, staggered post-match departure schedule using the formula:

```
T_departure = T_match_end + (ZoneDistance × CongestionWeight × 5 min)

ZoneDistance     = BFS hop-count from seating zone to nearest gate (static venue graph)
CongestionWeight = 1 + current_density   (range 1.0–2.0; higher density = later departure)
```

**Hard-Block Rule**: gate zones with `avg_density ≥ 90 %` (sourced from the `crowd_1min` continuous aggregate) are flagged as blocked. The planner automatically reroutes affected zones to the next nearest non-blocked gate, annotating the window with a `gate_was_rerouted: true` flag and a human-readable `note`.

Output is sorted by `departure_offset_minutes` (priority 1 = depart first). Each window is a 5-minute slot with ISO `window_start`/`window_end` timestamps. Query param `match_end_offset_minutes` (default `0`) lets ops simulate future kick-off times.

```bash
# Immediate exit plan (match just ended)
curl http://localhost:8000/ops/exit-plan | jq .

# Simulate match ending in 45 minutes
curl "http://localhost:8000/ops/exit-plan?match_end_offset_minutes=45" | jq .
```

#### `POST /ops/alerts/resolve/{alert_id}`
Sets `resolved = TRUE` in the `alerts` hypertable for the given UUID. Returns **404** if no unresolved alert with that UUID exists, **422** for an invalid UUID, and **503** if the database is unavailable.

```bash
# Resolve a specific alert
curl -X POST http://localhost:8000/ops/alerts/resolve/3fa85f64-5717-4562-b3fc-2c963f66afa6

# Check remaining critical alerts via health endpoint
curl http://localhost:8000/health | jq .unresolved_critical_alerts
```