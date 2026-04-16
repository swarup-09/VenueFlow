-- ─────────────────────────────────────────────────────────────────────────────
-- VenueFlow Phase 2 – TimescaleDB Schema
-- Digital Twin: persistent live state of the venue
-- ─────────────────────────────────────────────────────────────────────────────

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ── Raw time-series tables ────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS crowd_readings (
    time            TIMESTAMPTZ NOT NULL,
    sensor_id       TEXT        NOT NULL,
    zone            TEXT        NOT NULL,
    density         FLOAT       NOT NULL CHECK (density >= 0 AND density <= 1),
    headcount       INT         NOT NULL,
    flow_rate       FLOAT       NOT NULL,
    direction       TEXT        NOT NULL
);
SELECT create_hypertable('crowd_readings', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_crowd_zone ON crowd_readings (zone, time DESC);

CREATE TABLE IF NOT EXISTS queue_readings (
    time            TIMESTAMPTZ NOT NULL,
    sensor_id       TEXT        NOT NULL,
    stand_id        TEXT        NOT NULL,
    stand_name      TEXT        NOT NULL,
    zone            TEXT        NOT NULL,
    queue_length    INT         NOT NULL,
    wait_minutes    FLOAT       NOT NULL,
    capacity_pct    FLOAT       NOT NULL,
    is_open         BOOLEAN     NOT NULL DEFAULT TRUE
);
SELECT create_hypertable('queue_readings', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_queue_stand ON queue_readings (stand_id, time DESC);

CREATE TABLE IF NOT EXISTS ble_readings (
    time            TIMESTAMPTZ NOT NULL,
    beacon_id       TEXT        NOT NULL,
    location_label  TEXT        NOT NULL,
    zone            TEXT        NOT NULL,
    active_devices  INT         NOT NULL,
    rssi_avg        FLOAT       NOT NULL,
    dwell_time_avg  FLOAT       NOT NULL
);
SELECT create_hypertable('ble_readings', 'time', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS cctv_readings (
    time                TIMESTAMPTZ NOT NULL,
    camera_id           TEXT        NOT NULL,
    zone                TEXT        NOT NULL,
    headcount_estimate  INT         NOT NULL,
    motion_intensity    FLOAT       NOT NULL,
    anomaly_detected    BOOLEAN     NOT NULL DEFAULT FALSE,
    anomaly_type        TEXT
);
SELECT create_hypertable('cctv_readings', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_cctv_anomaly ON cctv_readings (anomaly_detected, time DESC);

-- ── Intelligence output tables ────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS flow_predictions (
    time            TIMESTAMPTZ NOT NULL,
    zone            TEXT        NOT NULL,
    predicted_density   FLOAT,
    recommended_route   TEXT,
    congestion_risk     TEXT,           -- low | moderate | high | critical
    avoid_zones         TEXT[]
);
SELECT create_hypertable('flow_predictions', 'time', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS wait_predictions (
    time            TIMESTAMPTZ NOT NULL,
    stand_id        TEXT        NOT NULL,
    stand_name      TEXT        NOT NULL,
    predicted_wait_5min     FLOAT,     -- wait in 5 minutes
    predicted_wait_10min    FLOAT,     -- wait in 10 minutes
    arrival_rate_lambda     FLOAT,     -- λ arrivals/min
    service_rate_mu         FLOAT,     -- μ service/min
    server_count            INT,       -- c servers (tills open)
    utilization_rho         FLOAT,     -- ρ = λ/(c·μ)
    halftime_spike_expected BOOLEAN
);
SELECT create_hypertable('wait_predictions', 'time', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS alerts (
    time        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    alert_id    UUID        DEFAULT gen_random_uuid(),
    severity    TEXT        NOT NULL,  -- info | warning | critical
    category    TEXT        NOT NULL,  -- crowd | queue | cctv | intelligence
    zone        TEXT,
    stand_id    TEXT,
    message     TEXT        NOT NULL,
    resolved    BOOLEAN     DEFAULT FALSE
);
SELECT create_hypertable('alerts', 'time', if_not_exists => TRUE);

-- ── Digital Twin: materialised live state ─────────────────────────────────────
-- One row per zone / stand reflecting LATEST reading only.

CREATE TABLE IF NOT EXISTS digital_twin_zones (
    zone                TEXT        PRIMARY KEY,
    last_updated        TIMESTAMPTZ NOT NULL,
    density             FLOAT,
    headcount           INT,
    flow_rate           FLOAT,
    direction           TEXT,
    congestion_level    TEXT,
    ble_active_devices  INT,
    cctv_headcount      INT,
    anomaly_active      BOOLEAN     DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS digital_twin_stands (
    stand_id            TEXT        PRIMARY KEY,
    stand_name          TEXT        NOT NULL,
    zone                TEXT        NOT NULL,
    last_updated        TIMESTAMPTZ NOT NULL,
    queue_length        INT,
    wait_minutes        FLOAT,
    capacity_pct        FLOAT,
    is_open             BOOLEAN     DEFAULT TRUE,
    predicted_wait_5min  FLOAT,
    predicted_wait_10min FLOAT,
    halftime_spike_expected BOOLEAN DEFAULT FALSE
);

-- Seed known stands so API can query even before first consumer message
INSERT INTO digital_twin_stands (stand_id, stand_name, zone, last_updated, queue_length, wait_minutes, capacity_pct)
VALUES
  ('C4', 'Hot Dogs & Snacks',  'Concourse-A', NOW(), 0, 0, 0),
  ('B2', 'Beer & Beverages',   'Concourse-B', NOW(), 0, 0, 0),
  ('A7', 'Pizza Corner',        'Concourse-A', NOW(), 0, 0, 0),
  ('B5', 'Burgers & Fries',    'Concourse-B', NOW(), 0, 0, 0),
  ('C1', 'Coffee & Desserts',  'Concourse-C', NOW(), 0, 0, 0),
  ('A3', 'Nachos & Wraps',     'Concourse-A', NOW(), 0, 0, 0),
  ('D2', 'VIP Bar',            'VIP-Lounge',  NOW(), 0, 0, 0),
  ('B8', 'South Grill',        'Concourse-B', NOW(), 0, 0, 0)
ON CONFLICT (stand_id) DO NOTHING;

-- ── Continuous aggregates (TimescaleDB feature) ───────────────────────────────
-- 1-minute bucketed crowd density per zone — used by flow predictor

CREATE MATERIALIZED VIEW IF NOT EXISTS crowd_1min
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', time) AS bucket,
    zone,
    AVG(density)    AS avg_density,
    MAX(headcount)  AS max_headcount,
    AVG(flow_rate)  AS avg_flow_rate
FROM crowd_readings
GROUP BY bucket, zone
WITH NO DATA;

SELECT add_continuous_aggregate_policy('crowd_1min',
    start_offset => INTERVAL '10 minutes',
    end_offset   => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists => TRUE
);

-- 1-minute queue averages per stand — used by wait estimator
CREATE MATERIALIZED VIEW IF NOT EXISTS queue_1min
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', time) AS bucket,
    stand_id,
    stand_name,
    AVG(queue_length)  AS avg_queue_length,
    AVG(wait_minutes)  AS avg_wait_minutes,
    AVG(capacity_pct)  AS avg_capacity_pct
FROM queue_readings
GROUP BY bucket, stand_id, stand_name
WITH NO DATA;

SELECT add_continuous_aggregate_policy('queue_1min',
    start_offset => INTERVAL '10 minutes',
    end_offset   => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists => TRUE
);

-- ── Phase 3: Fan Orders ───────────────────────────────────────────────────────
-- Stores mobile food orders placed via POST /fan/{seat_id}/order.
-- Hypertable partitioned by placed_at for fast time-range queries.

CREATE TABLE IF NOT EXISTS fan_orders (
    placed_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    order_id                UUID        NOT NULL,
    seat_id                 TEXT        NOT NULL,
    stand_id                TEXT        NOT NULL,
    stand_name              TEXT        NOT NULL,
    zone                    TEXT,                           -- fan's seating zone
    items                   JSONB       NOT NULL,           -- [{name, quantity, price}]
    status                  TEXT        NOT NULL DEFAULT 'placed',
                                                            -- placed | preparing | delivered | cancelled
    queue_wait_minutes      FLOAT,
    walk_minutes            FLOAT,
    delivery_eta_minutes    FLOAT,
    delivery_eta_timestamp  TIMESTAMPTZ,
    total_price             FLOAT,
    special_instructions    TEXT,
    updated_at              TIMESTAMPTZ DEFAULT NOW()
);

SELECT create_hypertable('fan_orders', 'placed_at', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_fan_orders_seat ON fan_orders (seat_id, placed_at DESC);
CREATE INDEX IF NOT EXISTS idx_fan_orders_status ON fan_orders (status, placed_at DESC);
CREATE INDEX IF NOT EXISTS idx_fan_orders_stand ON fan_orders (stand_id, placed_at DESC);
