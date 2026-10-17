-- Complete current historian schema. Edit this file for structural changes.
-- Run through schema.py, which checks database state before applying any SQL.
-- Existing databases use checked adoption, never replay this installation file.
-- Each statement runs separately for TimescaleDB continuous aggregate support.

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TYPE "status" AS ENUM ('ACTIVE', 'DISABLED');
CREATE TYPE "role" AS ENUM ('USER', 'ADMIN', 'OPERATIONS');

-- A domain keeps version validation compatible with compressed readings.
CREATE DOMAIN model_version_text AS TEXT
    CHECK (VALUE ~ '^[0-9]+\.[0-9]+\.[0-9]+$');

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(256) NOT NULL UNIQUE,
    first_name VARCHAR(256),
    last_name VARCHAR(256),
    user_name VARCHAR(256) NOT NULL UNIQUE,
    phone_number VARCHAR(256) UNIQUE,
    bio TEXT,
    image VARCHAR(256),
    role "role" DEFAULT 'USER',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status "status" DEFAULT 'ACTIVE',
    deleted BOOLEAN DEFAULT FALSE,
    deleted_at TIMESTAMPTZ,
    encrypted_password VARCHAR(256) NOT NULL,
    last_signed_in TIMESTAMPTZ,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    session_version INTEGER NOT NULL DEFAULT 0
);

-- Asset hierarchy -----------------------------------------------------------

CREATE TABLE field (
    field_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT
);

CREATE TABLE location (
    location_id SERIAL PRIMARY KEY,
    field_id INT NOT NULL REFERENCES field(field_id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    address TEXT,
    latitude FLOAT,
    longitude FLOAT
);

CREATE TABLE device (
    device_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    modbus_unit_id INT NOT NULL,
    status VARCHAR(50) DEFAULT 'active'
);

CREATE TABLE wellHead (
    wellhead_id SERIAL PRIMARY KEY,
    location_id INT NOT NULL REFERENCES location(location_id) ON DELETE CASCADE,
    device_id INT UNIQUE NOT NULL REFERENCES device(device_id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL UNIQUE,
    type VARCHAR(100),
    status VARCHAR(50) DEFAULT 'active'
);

-- Parameter metadata --------------------------------------------------------

CREATE TABLE parameterType (
    parameter_type_id SERIAL PRIMARY KEY,
    code VARCHAR(100) NOT NULL UNIQUE,
    display_name VARCHAR(255) NOT NULL,
    canonical_unit VARCHAR(50),
    data_type VARCHAR(50) NOT NULL,
    normal_min FLOAT,
    normal_max FLOAT
);

CREATE TABLE deviceParameterMapping (
    mapping_id SERIAL PRIMARY KEY,
    device_id INT NOT NULL REFERENCES device(device_id) ON DELETE CASCADE,
    parameter_type_id INT NOT NULL REFERENCES parameterType(parameter_type_id) ON DELETE CASCADE,
    modbus_register INT NOT NULL,
    function_code SMALLINT NOT NULL,
    register_type VARCHAR(50) NOT NULL,
    active BOOLEAN DEFAULT TRUE,
    UNIQUE(device_id, modbus_register)
);

-- Time-series ---------------------------------------------------------------

CREATE TABLE parameterReading (
    parameter_reading_id BIGSERIAL,
    timestamp_utc TIMESTAMPTZ NOT NULL,
    wellhead_id INT NOT NULL REFERENCES wellHead(wellhead_id) ON DELETE CASCADE,
    parameter_type_id INT NOT NULL REFERENCES parameterType(parameter_type_id) ON DELETE CASCADE,
    mapping_id INT NOT NULL REFERENCES deviceParameterMapping(mapping_id) ON DELETE CASCADE,
    raw_value FLOAT NOT NULL,
    inserted_at TIMESTAMPTZ DEFAULT NOW(),
    source_kind TEXT NOT NULL DEFAULT 'synthetic_random_simulator'
        CONSTRAINT parameter_reading_source_kind_check CHECK (source_kind IN (
            'synthetic_random_simulator', 'synthetic_reduced_order_model'
        )),
    model_version model_version_text,
    PRIMARY KEY (parameter_reading_id, timestamp_utc)
);

SELECT create_hypertable('parameterReading', 'timestamp_utc', if_not_exists => TRUE);

-- Every per-wellhead query filters on exactly this pair; no index covered it.
CREATE INDEX ix_parameter_reading_wellhead_time
    ON parameterReading (wellhead_id, timestamp_utc DESC);

CREATE INDEX ix_parameter_reading_type_time
    ON parameterReading (parameter_type_id, timestamp_utc DESC);

-- Alarms --------------------------------------------------------------------

CREATE TABLE alarmRule (
    alarm_rule_id SERIAL PRIMARY KEY,
    parameter_type_id INT NOT NULL REFERENCES parameterType(parameter_type_id) ON DELETE CASCADE,
    severity_level VARCHAR(50) NOT NULL,
    operator VARCHAR(10) NOT NULL,
    threshold_value FLOAT NOT NULL,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    trigger_after INT NOT NULL DEFAULT 3,
    clear_after INT NOT NULL DEFAULT 3
);

CREATE UNIQUE INDEX uq_alarm_rule_definition
    ON alarmRule (parameter_type_id, severity_level, operator, threshold_value);

CREATE TABLE alarmEvent (
    event_id BIGSERIAL NOT NULL,
    alarm_rule_id INT NOT NULL REFERENCES alarmRule(alarm_rule_id) ON DELETE CASCADE,
    parameter_reading_id BIGINT NOT NULL,
    timestamp_utc TIMESTAMPTZ NOT NULL,
    wellhead_id INT NOT NULL REFERENCES wellHead(wellhead_id) ON DELETE CASCADE,
    triggered_at TIMESTAMPTZ NOT NULL,
    cleared_at TIMESTAMPTZ,
    severity_level VARCHAR(50) NOT NULL,
    triggered_value FLOAT NOT NULL,
    PRIMARY KEY (event_id, triggered_at)
);

SELECT create_hypertable('alarmEvent', 'triggered_at', if_not_exists => TRUE);

-- The active-alarms view filters on exactly this predicate.
CREATE INDEX ix_alarm_event_open
    ON alarmEvent (wellhead_id, alarm_rule_id) WHERE cleared_at IS NULL;


-- Alarm evaluation with consecutive breach and clear counters.

COMMENT ON COLUMN alarmRule.trigger_after IS
    'Consecutive breaching evaluations required before the alarm opens.';
COMMENT ON COLUMN alarmRule.clear_after IS
    'Consecutive non-breaching evaluations required before the alarm clears.';

-- Per wellhead and rule, how many consecutive evaluations have agreed. Keeping
-- the counters here makes evaluation O(1) per rule instead of a scan back
-- through reading history.
CREATE TABLE alarmRuleState (
    wellhead_id INT NOT NULL REFERENCES wellHead(wellhead_id) ON DELETE CASCADE,
    alarm_rule_id INT NOT NULL REFERENCES alarmRule(alarm_rule_id) ON DELETE CASCADE,
    consecutive_breaches INT NOT NULL DEFAULT 0,
    consecutive_clears INT NOT NULL DEFAULT 0,
    last_evaluated_at TIMESTAMPTZ,
    PRIMARY KEY (wellhead_id, alarm_rule_id)
);

CREATE OR REPLACE FUNCTION evaluate_wellhead_alarms()
RETURNS TRIGGER AS $$
BEGIN
    -- One evaluation per (wellhead, rule) per statement. The ingestion service
    -- writes one reading per wellhead and parameter per batch, so the latest
    -- row in the batch is that evaluation. A batch carrying several samples for
    -- the same parameter counts as one evaluation, which delays a state change
    -- rather than causing a wrong one.
    WITH latest AS (
        SELECT DISTINCT ON (i.wellhead_id, i.parameter_type_id)
               i.wellhead_id,
               i.parameter_type_id,
               i.parameter_reading_id,
               i.timestamp_utc,
               i.raw_value
        FROM inserted i
        ORDER BY i.wellhead_id, i.parameter_type_id, i.timestamp_utc DESC
    ),
    evaluated AS (
        SELECT l.wellhead_id,
               r.alarm_rule_id,
               r.severity_level,
               r.trigger_after,
               r.clear_after,
               l.parameter_reading_id,
               l.timestamp_utc,
               l.raw_value,
               CASE r.operator
                   WHEN '>'  THEN l.raw_value > r.threshold_value
                   WHEN '>=' THEN l.raw_value >= r.threshold_value
                   WHEN '<'  THEN l.raw_value < r.threshold_value
                   WHEN '<=' THEN l.raw_value <= r.threshold_value
                   WHEN '==' THEN l.raw_value = r.threshold_value
                   ELSE FALSE
               END AS breaching
        FROM latest l
        JOIN alarmRule r
          ON r.parameter_type_id = l.parameter_type_id
         AND r.active = TRUE
    ),
    counted AS (
        INSERT INTO alarmRuleState AS s (
            wellhead_id, alarm_rule_id,
            consecutive_breaches, consecutive_clears, last_evaluated_at
        )
        SELECT e.wellhead_id,
               e.alarm_rule_id,
               CASE WHEN e.breaching THEN 1 ELSE 0 END,
               CASE WHEN e.breaching THEN 0 ELSE 1 END,
               e.timestamp_utc
        FROM evaluated e
        ON CONFLICT (wellhead_id, alarm_rule_id) DO UPDATE
        SET consecutive_breaches = CASE
                WHEN EXCLUDED.consecutive_breaches > 0
                THEN s.consecutive_breaches + 1 ELSE 0 END,
            consecutive_clears = CASE
                WHEN EXCLUDED.consecutive_clears > 0
                THEN s.consecutive_clears + 1 ELSE 0 END,
            last_evaluated_at = EXCLUDED.last_evaluated_at
        RETURNING s.wellhead_id, s.alarm_rule_id,
                  s.consecutive_breaches, s.consecutive_clears
    ),
    decided AS (
        SELECT e.*, c.consecutive_breaches, c.consecutive_clears
        FROM evaluated e
        JOIN counted c
          ON c.wellhead_id = e.wellhead_id
         AND c.alarm_rule_id = e.alarm_rule_id
    ),
    opened AS (
        INSERT INTO alarmEvent (
            alarm_rule_id, parameter_reading_id, wellhead_id, timestamp_utc,
            triggered_at, severity_level, triggered_value
        )
        SELECT d.alarm_rule_id, d.parameter_reading_id, d.wellhead_id,
               d.timestamp_utc, d.timestamp_utc, d.severity_level, d.raw_value
        FROM decided d
        WHERE d.breaching
          AND d.consecutive_breaches >= d.trigger_after
          AND NOT EXISTS (
              SELECT 1 FROM alarmEvent a
              WHERE a.wellhead_id = d.wellhead_id
                AND a.alarm_rule_id = d.alarm_rule_id
                AND a.cleared_at IS NULL
          )
        RETURNING 1
    )
    UPDATE alarmEvent a
    SET cleared_at = d.timestamp_utc
    FROM decided d
    WHERE a.wellhead_id = d.wellhead_id
      AND a.alarm_rule_id = d.alarm_rule_id
      AND a.cleared_at IS NULL
      AND NOT d.breaching
      AND d.consecutive_clears >= d.clear_after;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER evaluate_wellhead_alarms_trigger
AFTER INSERT ON parameterReading
REFERENCING NEW TABLE AS inserted
FOR EACH STATEMENT
EXECUTE FUNCTION evaluate_wellhead_alarms();



-- Dashboard read views.

CREATE VIEW v_wellhead_parameter_readings AS
SELECT
    pr.timestamp_utc,
    pr.raw_value,
    wh.wellhead_id,
    wh.name AS wellhead_name,
    wh.type AS wellhead_type,
    loc.location_id,
    loc.name AS location_name,
    f.field_id,
    f.name AS field_name,
    pt.parameter_type_id,
    pt.code AS parameter_code,
    pt.display_name AS parameter_display_name,
    pt.canonical_unit,
    pt.data_type,
    pt.normal_min,
    pt.normal_max
FROM parameterReading pr
JOIN wellHead wh ON pr.wellhead_id = wh.wellhead_id
JOIN location loc ON wh.location_id = loc.location_id
JOIN field f ON loc.field_id = f.field_id
JOIN parameterType pt ON pr.parameter_type_id = pt.parameter_type_id;

-- Open alarms with the threshold context needed to explain why they fired.
CREATE VIEW v_active_alarms AS
SELECT
    ae.event_id,
    ae.triggered_at,
    ae.severity_level,
    ae.triggered_value,
    wh.wellhead_id,
    wh.name AS wellhead_name,
    loc.name AS location_name,
    f.name AS field_name,
    pt.code AS parameter_code,
    pt.display_name AS parameter_display_name,
    pt.canonical_unit,
    ar.operator,
    ar.threshold_value,
    ar.alarm_rule_id
FROM alarmEvent ae
JOIN wellHead wh ON ae.wellhead_id = wh.wellhead_id
JOIN location loc ON wh.location_id = loc.location_id
JOIN field f ON loc.field_id = f.field_id
JOIN alarmRule ar ON ae.alarm_rule_id = ar.alarm_rule_id
JOIN parameterType pt ON ar.parameter_type_id = pt.parameter_type_id
WHERE ae.cleared_at IS NULL;


-- Trend summaries and their refresh policies.

CREATE MATERIALIZED VIEW mv_2min_pressure_trends
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('2 minutes', pr.timestamp_utc) AS bucket_time,
    pr.wellhead_id,
    wh.name AS wellhead_name,
    loc.name AS location_name,
    f.name AS field_name,
    pt.code AS parameter_code,
    pt.display_name AS parameter_display_name,
    pt.canonical_unit,
    AVG(pr.raw_value) AS avg_value,
    MIN(pr.raw_value) AS min_value,
    MAX(pr.raw_value) AS max_value,
    COUNT(pr.raw_value) AS reading_count
FROM parameterReading pr
JOIN wellHead wh ON pr.wellhead_id = wh.wellhead_id
JOIN location loc ON wh.location_id = loc.location_id
JOIN field f ON loc.field_id = f.field_id
JOIN parameterType pt ON pr.parameter_type_id = pt.parameter_type_id
WHERE pt.code IN ('tubing_pressure', 'casing_pressure', 'annulus_pressure')
GROUP BY 1, pr.wellhead_id, wh.name, loc.name, f.name,
         pt.code, pt.display_name, pt.canonical_unit;

SELECT add_continuous_aggregate_policy('mv_2min_pressure_trends',
    start_offset => INTERVAL '1 day',
    end_offset => INTERVAL '2 minutes',
    schedule_interval => INTERVAL '2 minutes');

CREATE MATERIALIZED VIEW mv_2min_temp_flow_trends
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('2 minutes', pr.timestamp_utc) AS bucket_time,
    pr.wellhead_id,
    wh.name AS wellhead_name,
    loc.name AS location_name,
    f.name AS field_name,
    pt.code AS parameter_code,
    pt.display_name AS parameter_display_name,
    pt.canonical_unit,
    AVG(pr.raw_value) AS avg_value,
    MIN(pr.raw_value) AS min_value,
    MAX(pr.raw_value) AS max_value,
    COUNT(pr.raw_value) AS reading_count
FROM parameterReading pr
JOIN wellHead wh ON pr.wellhead_id = wh.wellhead_id
JOIN location loc ON wh.location_id = loc.location_id
JOIN field f ON loc.field_id = f.field_id
JOIN parameterType pt ON pr.parameter_type_id = pt.parameter_type_id
WHERE pt.code IN ('wellhead_temperature', 'flow_rate')
GROUP BY 1, pr.wellhead_id, wh.name, loc.name, f.name,
         pt.code, pt.display_name, pt.canonical_unit;

SELECT add_continuous_aggregate_policy('mv_2min_temp_flow_trends',
    start_offset => INTERVAL '1 day',
    end_offset => INTERVAL '2 minutes',
    schedule_interval => INTERVAL '2 minutes');

CREATE MATERIALIZED VIEW mv_2min_water_cut_gor_trends
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('2 minutes', pr.timestamp_utc) AS bucket_time,
    pr.wellhead_id,
    wh.name AS wellhead_name,
    loc.name AS location_name,
    f.name AS field_name,
    pt.code AS parameter_code,
    pt.display_name AS parameter_display_name,
    pt.canonical_unit,
    AVG(pr.raw_value) AS avg_value,
    MIN(pr.raw_value) AS min_value,
    MAX(pr.raw_value) AS max_value,
    COUNT(pr.raw_value) AS reading_count
FROM parameterReading pr
JOIN wellHead wh ON pr.wellhead_id = wh.wellhead_id
JOIN location loc ON wh.location_id = loc.location_id
JOIN field f ON loc.field_id = f.field_id
JOIN parameterType pt ON pr.parameter_type_id = pt.parameter_type_id
WHERE pt.code IN ('water_cut', 'gas_oil_ratio')
GROUP BY 1, pr.wellhead_id, wh.name, loc.name, f.name,
         pt.code, pt.display_name, pt.canonical_unit;

SELECT add_continuous_aggregate_policy('mv_2min_water_cut_gor_trends',
    start_offset => INTERVAL '1 day',
    end_offset => INTERVAL '2 minutes',
    schedule_interval => INTERVAL '2 minutes');

CREATE MATERIALIZED VIEW mv_5min_alarm_counts
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', ae.triggered_at) AS bucket_time,
    ae.wellhead_id,
    wh.name AS wellhead_name,
    loc.name AS location_name,
    pt.display_name AS parameter_display_name,
    ae.severity_level,
    COUNT(ae.event_id) AS alarms_triggered
FROM alarmEvent ae
JOIN wellHead wh ON ae.wellhead_id = wh.wellhead_id
JOIN location loc ON wh.location_id = loc.location_id
JOIN alarmRule ar ON ae.alarm_rule_id = ar.alarm_rule_id
JOIN parameterType pt ON ar.parameter_type_id = pt.parameter_type_id
GROUP BY 1, ae.wellhead_id, wh.name, loc.name,
         pt.display_name, ae.severity_level;

SELECT add_continuous_aggregate_policy('mv_5min_alarm_counts',
    start_offset => INTERVAL '30 days',
    end_offset => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes');


--
-- Tiered retention and compression, implementing ADR 0026.
--
-- Intervals are sized for a demonstrable portfolio system rather than a
-- production historian: at a five-second interval, 12 wellheads and 18
-- parameters produce roughly 3.7 million rows per day, so raw readings are
-- compressed quickly and dropped after a week. The continuous aggregates keep
-- the longer-range trend.

ALTER TABLE parameterReading SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'wellhead_id, parameter_type_id',
    timescaledb.compress_orderby = 'timestamp_utc DESC'
);

ALTER TABLE alarmEvent SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'wellhead_id, alarm_rule_id',
    timescaledb.compress_orderby = 'triggered_at DESC'
);

-- Compress raw readings once they are older than the trend windows that read
-- them uncompressed.
SELECT add_compression_policy('parameterReading', INTERVAL '1 day',
    if_not_exists => TRUE);
SELECT add_compression_policy('alarmEvent', INTERVAL '7 days',
    if_not_exists => TRUE);

SELECT add_retention_policy('parameterReading', INTERVAL '7 days',
    if_not_exists => TRUE);
SELECT add_retention_policy('alarmEvent', INTERVAL '90 days',
    if_not_exists => TRUE);

--
-- The audit trail, implementing ADR 0034.
--
-- Nothing in this system recorded who did anything. This table answers who did
-- what, when, and to which record. It is built for the person operating the
-- system rather than for a regulator, so there is no hash chain and no separate
-- store. ADR 0034 records why.

CREATE TABLE auditEvent (
    audit_event_id BIGSERIAL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Null when no known user acted, which is every failed login against an
    -- account that does not exist.
    actor_user_id INT REFERENCES users(id) ON DELETE SET NULL,

    -- What the caller submitted. Recorded whether or not it matches an account,
    -- so the presence of a row cannot be used to discover which accounts exist.
    -- Never populated from the password field.
    actor_identifier VARCHAR(256),

    action VARCHAR(64) NOT NULL,
    subject_type VARCHAR(64),
    subject_id VARCHAR(64),
    outcome VARCHAR(16) NOT NULL,

    -- Ties the row to one request and to the log lines that request produced.
    request_id VARCHAR(64),

    detail JSONB NOT NULL DEFAULT '{}'::jsonb,

    PRIMARY KEY (audit_event_id, occurred_at),
    CONSTRAINT audit_event_outcome CHECK (outcome IN ('success', 'failure'))
);

SELECT create_hypertable('auditEvent', 'occurred_at', if_not_exists => TRUE);

-- Reading the trail means asking what one actor did, or what happened to one
-- subject, almost always most recent first.
CREATE INDEX ix_audit_event_actor
    ON auditEvent (actor_user_id, occurred_at DESC);
CREATE INDEX ix_audit_event_action
    ON auditEvent (action, occurred_at DESC);
CREATE INDEX ix_audit_event_subject
    ON auditEvent (subject_type, subject_id, occurred_at DESC);

-- Failed logins arrive from unauthenticated traffic, so the identifier is the
-- column an investigation starts from.
CREATE INDEX ix_audit_event_identifier
    ON auditEvent (actor_identifier, occurred_at DESC)
    WHERE actor_identifier IS NOT NULL;

SELECT add_retention_policy('auditEvent', INTERVAL '90 days',
    if_not_exists => TRUE);

-- One indexed lookup per active mapping keeps the twin-core's latest-reading
-- endpoint bounded as historian data grows.
CREATE INDEX ix_parameter_reading_mapping_time
    ON parameterReading (mapping_id, timestamp_utc DESC, parameter_reading_id DESC);

-- Milestone 7: append-only model snapshots, recovery events, and audited
-- per-well parameter overrides. Policy functions run as separate statements.

CREATE TABLE twin_state_snapshot (
    snapshot_id BIGSERIAL NOT NULL,
    simulated_at TIMESTAMPTZ NOT NULL,
    wellhead_id INT NOT NULL REFERENCES wellhead(wellhead_id),
    model_version model_version_text NOT NULL,
    parameter_fingerprint TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status = 'running'),
    tick_index BIGINT NOT NULL CHECK (tick_index >= 0),
    skipped_ticks BIGINT NOT NULL CHECK (skipped_ticks >= 0),
    tubing_pressure_psi DOUBLE PRECISION NOT NULL,
    casing_pressure_psi DOUBLE PRECISION NOT NULL,
    flow_rate_bpd DOUBLE PRECISION NOT NULL,
    wellhead_temperature_f DOUBLE PRECISION NOT NULL,
    water_cut_percent DOUBLE PRECISION NOT NULL,
    choke_position_percent DOUBLE PRECISION NOT NULL,
    blockage_factor DOUBLE PRECISION NOT NULL,
    corrosion_factor DOUBLE PRECISION NOT NULL,
    state JSONB NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (snapshot_id, simulated_at)
);
SELECT create_hypertable('twin_state_snapshot', 'simulated_at', if_not_exists => TRUE);
CREATE INDEX ix_twin_state_snapshot_well_time
    ON twin_state_snapshot (wellhead_id, simulated_at DESC);
ALTER TABLE twin_state_snapshot SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'wellhead_id',
    timescaledb.compress_orderby = 'simulated_at DESC'
);
SELECT add_compression_policy('twin_state_snapshot', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('twin_state_snapshot', INTERVAL '90 days', if_not_exists => TRUE);

CREATE TABLE twin_recovery_event (
    event_id BIGSERIAL NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    wellhead_id INT REFERENCES wellhead(wellhead_id),
    event_type TEXT NOT NULL CHECK (event_type IN (
        'snapshot_restored', 'cold_start_no_snapshot', 'snapshot_incompatible',
        'clock_jump_forward', 'clock_jump_backward', 'active_controls_restored'
    )),
    model_version model_version_text NOT NULL,
    snapshot_simulated_at TIMESTAMPTZ,
    gap_seconds DOUBLE PRECISION CHECK (gap_seconds IS NULL OR gap_seconds >= 0),
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (event_id, occurred_at)
);
SELECT create_hypertable('twin_recovery_event', 'occurred_at', if_not_exists => TRUE);
CREATE INDEX ix_twin_recovery_event_well_time
    ON twin_recovery_event (wellhead_id, occurred_at DESC);
SELECT add_retention_policy('twin_recovery_event', INTERVAL '2 years', if_not_exists => TRUE);

CREATE TABLE wellhead_model_parameter (
    parameter_override_id SERIAL PRIMARY KEY,
    wellhead_id INT NOT NULL REFERENCES wellhead(wellhead_id),
    model_version model_version_text NOT NULL,
    parameter_key TEXT NOT NULL,
    value DOUBLE PRECISION,
    unit TEXT,
    source TEXT NOT NULL CHECK (length(trim(source)) > 0),
    changed_by TEXT NOT NULL CHECK (length(trim(changed_by)) > 0),
    reason TEXT NOT NULL CHECK (length(trim(reason)) > 0),
    valid_from TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((value IS NULL) = (unit IS NULL)),
    UNIQUE (wellhead_id, model_version, parameter_key, valid_from)
);
CREATE INDEX ix_wellhead_model_parameter_current
    ON wellhead_model_parameter (wellhead_id, model_version, parameter_key, valid_from DESC);

-- Milestone 8: complete, attributed model projections. Runs are ordinary
-- tables; sampled future points are hypertables with time in each primary key.

CREATE TABLE prediction_run (
    run_id UUID PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL,
    wellhead_id INT NOT NULL REFERENCES wellhead(wellhead_id),
    requested_by_user_id INT NOT NULL REFERENCES users(id),
    model_version model_version_text NOT NULL,
    parameter_fingerprint TEXT NOT NULL,
    base_simulated_at TIMESTAMPTZ NOT NULL,
    horizon_seconds INT NOT NULL CHECK (horizon_seconds BETWEEN 300 AND 7200),
    step_seconds INT NOT NULL CHECK (step_seconds BETWEEN 5 AND 300),
    base_state JSONB NOT NULL,
    parameter_values JSONB NOT NULL,
    control_values JSONB NOT NULL,
    alarm_rules JSONB NOT NULL,
    confidence_score DOUBLE PRECISION NOT NULL CHECK (confidence_score BETWEEN 0 AND 1),
    confidence_reasons JSONB NOT NULL
);
CREATE INDEX ix_prediction_run_well_created
    ON prediction_run (wellhead_id, created_at DESC);

CREATE TABLE prediction_point (
    run_id UUID NOT NULL REFERENCES prediction_run(run_id),
    forecast_at TIMESTAMPTZ NOT NULL,
    elapsed_seconds INT NOT NULL CHECK (elapsed_seconds > 0),
    values_json JSONB NOT NULL,
    tubing_pressure_psi DOUBLE PRECISION NOT NULL CHECK (tubing_pressure_psi >= 0),
    flow_rate_bpd DOUBLE PRECISION NOT NULL CHECK (flow_rate_bpd >= 0),
    wellhead_temperature_f DOUBLE PRECISION NOT NULL,
    water_cut_percent DOUBLE PRECISION NOT NULL CHECK (water_cut_percent BETWEEN 0 AND 100),
    alarm_risk TEXT NOT NULL CHECK (alarm_risk IN ('none', 'warning', 'critical', 'unknown')),
    triggered_rule_ids JSONB NOT NULL,
    PRIMARY KEY (run_id, forecast_at)
);
SELECT create_hypertable('prediction_point', 'forecast_at', if_not_exists => TRUE);
ALTER TABLE prediction_point SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'run_id',
    timescaledb.compress_orderby = 'forecast_at DESC'
);
SELECT add_compression_policy('prediction_point', INTERVAL '3 days', if_not_exists => TRUE);
SELECT add_retention_policy('prediction_point', INTERVAL '30 days', if_not_exists => TRUE);

CREATE TABLE scenario_run (
    run_id UUID PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL,
    wellhead_id INT NOT NULL REFERENCES wellhead(wellhead_id),
    requested_by_user_id INT NOT NULL REFERENCES users(id),
    name VARCHAR(120) NOT NULL CHECK (length(trim(name)) > 0),
    model_version model_version_text NOT NULL,
    parameter_fingerprint TEXT NOT NULL,
    base_simulated_at TIMESTAMPTZ NOT NULL,
    horizon_seconds INT NOT NULL CHECK (horizon_seconds BETWEEN 300 AND 7200),
    step_seconds INT NOT NULL CHECK (step_seconds BETWEEN 5 AND 300),
    base_state JSONB NOT NULL,
    parameter_values JSONB NOT NULL,
    control_values JSONB NOT NULL,
    scheduled_controls JSONB NOT NULL,
    alarm_rules JSONB NOT NULL,
    confidence_score DOUBLE PRECISION NOT NULL CHECK (confidence_score BETWEEN 0 AND 1),
    confidence_reasons JSONB NOT NULL
);
CREATE INDEX ix_scenario_run_well_created
    ON scenario_run (wellhead_id, created_at DESC);

CREATE TABLE scenario_point (
    run_id UUID NOT NULL REFERENCES scenario_run(run_id),
    forecast_at TIMESTAMPTZ NOT NULL,
    elapsed_seconds INT NOT NULL CHECK (elapsed_seconds > 0),
    values_json JSONB NOT NULL,
    tubing_pressure_psi DOUBLE PRECISION NOT NULL CHECK (tubing_pressure_psi >= 0),
    flow_rate_bpd DOUBLE PRECISION NOT NULL CHECK (flow_rate_bpd >= 0),
    wellhead_temperature_f DOUBLE PRECISION NOT NULL,
    water_cut_percent DOUBLE PRECISION NOT NULL CHECK (water_cut_percent BETWEEN 0 AND 100),
    baseline_values_json JSONB NOT NULL,
    alarm_risk TEXT NOT NULL CHECK (alarm_risk IN ('none', 'warning', 'critical', 'unknown')),
    baseline_alarm_risk TEXT NOT NULL CHECK (baseline_alarm_risk IN ('none', 'warning', 'critical', 'unknown')),
    triggered_rule_ids JSONB NOT NULL,
    PRIMARY KEY (run_id, forecast_at)
);
SELECT create_hypertable('scenario_point', 'forecast_at', if_not_exists => TRUE);
ALTER TABLE scenario_point SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'run_id',
    timescaledb.compress_orderby = 'forecast_at DESC'
);
SELECT add_compression_policy('scenario_point', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('scenario_point', INTERVAL '90 days', if_not_exists => TRUE);

-- Durable synthetic control commands. No field-device write path is added.
CREATE TABLE control_command (
    command_id UUID PRIMARY KEY,
    wellhead_id INT NOT NULL REFERENCES wellhead(wellhead_id),
    created_by INT NOT NULL REFERENCES users(id),
    idempotency_key UUID NOT NULL,
    reason VARCHAR(500) NOT NULL CHECK (length(trim(reason)) > 0),
    expected_revision INT NOT NULL CHECK (expected_revision >= 0),
    model_version model_version_text NOT NULL,
    parameter_fingerprint TEXT NOT NULL,
    choke_position_percent DOUBLE PRECISION CHECK (choke_position_percent BETWEEN 0 AND 100),
    master_valve_open BOOLEAN,
    wing_valve_open BOOLEAN,
    pump_on BOOLEAN,
    emergency_shutdown BOOLEAN,
    status TEXT NOT NULL DEFAULT 'draft' CHECK (status IN
        ('draft', 'queued', 'applied', 'rejected', 'expired', 'cancelled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    expires_at TIMESTAMPTZ NOT NULL DEFAULT (clock_timestamp() + INTERVAL '24 hours'),
    queued_at TIMESTAMPTZ,
    activated_by INT REFERENCES users(id),
    activated_role TEXT,
    activated_session_version INT,
    result_at TIMESTAMPTZ,
    result_revision INT,
    result_reason VARCHAR(64),
    request_id VARCHAR(64),
    UNIQUE (created_by, idempotency_key),
    CHECK (num_nonnulls(choke_position_percent, master_valve_open,
        wing_valve_open, pump_on, emergency_shutdown) > 0),
    CHECK (emergency_shutdown IS NULL OR num_nonnulls(choke_position_percent,
        master_valve_open, wing_valve_open, pump_on) = 0),
    CHECK (status <> 'queued' OR (queued_at IS NOT NULL AND activated_by IS NOT NULL AND activated_by = created_by
        AND activated_role IS NOT NULL AND activated_session_version IS NOT NULL)),
    CHECK ((status IN ('applied', 'rejected', 'expired', 'cancelled')) = (result_at IS NOT NULL)),
    CHECK (status <> 'applied' OR (result_revision IS NOT NULL AND result_revision = expected_revision + 1))
);
CREATE UNIQUE INDEX ix_control_one_draft
    ON control_command(created_by, wellhead_id) WHERE status = 'draft';
CREATE UNIQUE INDEX ix_control_one_queued
    ON control_command(wellhead_id) WHERE status = 'queued';
CREATE INDEX ix_control_inbox ON control_command(queued_at, command_id)
    WHERE status = 'queued';

CREATE TABLE wellhead_control_state (
    wellhead_id INT PRIMARY KEY REFERENCES wellhead(wellhead_id),
    revision INT NOT NULL DEFAULT 0 CHECK (revision >= 0),
    model_version model_version_text NOT NULL,
    parameter_fingerprint TEXT NOT NULL,
    choke_position_percent DOUBLE PRECISION NOT NULL CHECK (choke_position_percent BETWEEN 0 AND 100),
    master_valve_open BOOLEAN NOT NULL,
    wing_valve_open BOOLEAN NOT NULL,
    swab_valve_open BOOLEAN NOT NULL,
    pump_on BOOLEAN NOT NULL,
    emergency_shutdown BOOLEAN NOT NULL,
    applied_command_id UUID REFERENCES control_command(command_id),
    applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE control_command_event (
    event_id BIGSERIAL PRIMARY KEY,
    command_id UUID NOT NULL REFERENCES control_command(command_id),
    event_type VARCHAR(32) NOT NULL CHECK (event_type IN
        ('draft', 'queued', 'applied', 'rejected', 'expired', 'cancelled')),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    actor_user_id INT REFERENCES users(id),
    reason_code VARCHAR(64) NOT NULL,
    request_id VARCHAR(64)
);
CREATE INDEX ix_control_event_command ON control_command_event(command_id, event_id);

CREATE TABLE model_parameter_change (
    change_id UUID PRIMARY KEY,
    wellhead_id INT NOT NULL REFERENCES wellhead(wellhead_id),
    created_by INT NOT NULL REFERENCES users(id),
    idempotency_key UUID NOT NULL,
    model_version model_version_text NOT NULL,
    parameter_key TEXT NOT NULL,
    value DOUBLE PRECISION,
    unit TEXT,
    reason VARCHAR(500) NOT NULL CHECK (length(trim(reason)) > 0),
    expected_override_id INT REFERENCES wellhead_model_parameter(parameter_override_id),
    status TEXT NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'published', 'cancelled', 'expired')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    expires_at TIMESTAMPTZ NOT NULL DEFAULT (clock_timestamp() + INTERVAL '24 hours'),
    result_at TIMESTAMPTZ,
    published_override_id INT REFERENCES wellhead_model_parameter(parameter_override_id),
    UNIQUE (created_by, idempotency_key),
    CHECK ((value IS NULL) = (unit IS NULL)),
    CHECK (value IS NULL OR (value > '-Infinity'::float8 AND value < 'Infinity'::float8)),
    CHECK ((status <> 'draft') = (result_at IS NOT NULL)),
    CHECK ((status = 'published') = (published_override_id IS NOT NULL))
);
CREATE UNIQUE INDEX ix_parameter_one_draft
    ON model_parameter_change(created_by, wellhead_id, parameter_key) WHERE status = 'draft';

-- Narrow lock helpers avoid granting twin-core user-update rights or granting
-- the API active-control-update rights merely to acquire a row lock.
CREATE FUNCTION control_actor_authority(actor_id INT)
RETURNS TABLE(role TEXT, status TEXT, deleted BOOLEAN, session_version INT)
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp
AS $$ SELECT role::text, status::text, deleted, session_version
      FROM public.users WHERE id = actor_id FOR SHARE $$;
REVOKE ALL ON FUNCTION control_actor_authority(INT) FROM PUBLIC;

CREATE FUNCTION lock_control_state(well_id INT) RETURNS VOID
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp
AS $$ SELECT 1 FROM public.wellhead_control_state WHERE wellhead_id = well_id FOR UPDATE $$;
REVOKE ALL ON FUNCTION lock_control_state(INT) FROM PUBLIC;

CREATE FUNCTION enforce_control_lifecycle() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, public, pg_temp AS $$
BEGIN
    IF (to_jsonb(NEW) - ARRAY['status','expires_at','queued_at','activated_by',
        'activated_role','activated_session_version','result_at','result_revision','result_reason'])
        IS DISTINCT FROM
       (to_jsonb(OLD) - ARRAY['status','expires_at','queued_at','activated_by',
        'activated_role','activated_session_version','result_at','result_revision','result_reason']) THEN
        RAISE EXCEPTION 'Control proposals are immutable';
    END IF;
    IF NOT ((OLD.status = 'draft' AND NEW.status IN ('queued','cancelled','expired'))
        OR (OLD.status = 'queued' AND NEW.status IN ('applied','rejected','expired','cancelled'))) THEN
        RAISE EXCEPTION 'Invalid control lifecycle transition';
    END IF;
    IF OLD.status = 'queued' AND
       ROW(NEW.queued_at, NEW.expires_at, NEW.activated_by, NEW.activated_role, NEW.activated_session_version)
       IS DISTINCT FROM
       ROW(OLD.queued_at, OLD.expires_at, OLD.activated_by, OLD.activated_role, OLD.activated_session_version) THEN
        RAISE EXCEPTION 'Activation authority is immutable';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER enforce_control_lifecycle BEFORE UPDATE ON control_command
    FOR EACH ROW EXECUTE FUNCTION enforce_control_lifecycle();

CREATE FUNCTION enforce_parameter_change_lifecycle() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, public, pg_temp AS $$
BEGIN
    IF (to_jsonb(NEW) - ARRAY['status','result_at','published_override_id'])
       IS DISTINCT FROM (to_jsonb(OLD) - ARRAY['status','result_at','published_override_id'])
       OR OLD.status <> 'draft' OR NEW.status NOT IN ('published','cancelled','expired') THEN
        RAISE EXCEPTION 'Invalid parameter change lifecycle transition';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER enforce_parameter_change_lifecycle BEFORE UPDATE ON model_parameter_change
    FOR EACH ROW EXECUTE FUNCTION enforce_parameter_change_lifecycle();

-- Scheduled forecasts: configuration and immutable claimed-work context.
CREATE TABLE forecast_schedule (
    schedule_id UUID PRIMARY KEY,
    wellhead_id INT NOT NULL UNIQUE REFERENCES wellhead(wellhead_id),
    owner_user_id INT NOT NULL REFERENCES users(id),
    owner_role TEXT NOT NULL CHECK (owner_role = 'ADMIN'),
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    interval_seconds INT NOT NULL CHECK (interval_seconds BETWEEN 300 AND 86400 AND interval_seconds % 60 = 0),
    horizon_seconds INT NOT NULL CHECK (horizon_seconds BETWEEN 300 AND 1800 AND horizon_seconds % 60 = 0),
    step_seconds INT NOT NULL DEFAULT 60 CHECK (step_seconds = 60),
    revision INT NOT NULL CHECK (revision > 0),
    next_due_at TIMESTAMPTZ,
    reason VARCHAR(500) NOT NULL CHECK (length(trim(reason)) > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (enabled = (next_due_at IS NOT NULL))
);
CREATE INDEX ix_forecast_schedule_due ON forecast_schedule(next_due_at, schedule_id) WHERE enabled;

CREATE TABLE forecast_job (
    job_id UUID PRIMARY KEY,
    schedule_id UUID NOT NULL REFERENCES forecast_schedule(schedule_id),
    schedule_revision INT NOT NULL CHECK (schedule_revision > 0),
    wellhead_id INT NOT NULL REFERENCES wellhead(wellhead_id),
    owner_user_id INT NOT NULL REFERENCES users(id),
    owner_role TEXT NOT NULL CHECK (owner_role = 'ADMIN'),
    interval_seconds INT NOT NULL CHECK (interval_seconds BETWEEN 300 AND 86400 AND interval_seconds % 60 = 0),
    horizon_seconds INT NOT NULL CHECK (horizon_seconds BETWEEN 300 AND 1800 AND horizon_seconds % 60 = 0),
    step_seconds INT NOT NULL CHECK (step_seconds = 60),
    scheduled_for TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    lease_expires_at TIMESTAMPTZ NOT NULL,
    missed_intervals INT NOT NULL CHECK (missed_intervals >= 0),
    status TEXT NOT NULL CHECK (status IN ('running', 'complete', 'skipped', 'failed')),
    completed_at TIMESTAMPTZ,
    reason_code VARCHAR(64),
    run_id UUID REFERENCES prediction_run(run_id),
    UNIQUE (schedule_id, scheduled_for),
    CHECK ((status = 'running') = (completed_at IS NULL)),
    CHECK ((status = 'complete') = (run_id IS NOT NULL)),
    CHECK (lease_expires_at > started_at)
);
CREATE UNIQUE INDEX ix_forecast_job_running ON forecast_job(schedule_id) WHERE status = 'running';
CREATE INDEX ix_forecast_job_recent ON forecast_job(wellhead_id, started_at DESC);
CREATE INDEX ix_forecast_job_expiry ON forecast_job(lease_expires_at) WHERE status = 'running';

-- Operator actions are independent of the retained alarm condition events.
CREATE TABLE alarm_action (
    action_id UUID PRIMARY KEY,
    actor_user_id INT NOT NULL,
    actor_name VARCHAR(256) NOT NULL,
    actor_role TEXT NOT NULL CHECK (actor_role = 'OPERATIONS'),
    request_key UUID NOT NULL,
    input_hash VARCHAR(64) NOT NULL CHECK (input_hash ~ '^[0-9a-f]{64}$'),
    action TEXT NOT NULL CHECK (action IN ('acknowledge', 'shelve', 'unshelve')),
    wellhead_id INT NOT NULL,
    alarm_rule_id INT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL CHECK (isfinite(occurred_at)),
    input JSONB NOT NULL CHECK (jsonb_typeof(input) = 'object'),
    result JSONB NOT NULL CHECK (jsonb_typeof(result) = 'object'),
    UNIQUE (actor_user_id, request_key)
);
CREATE INDEX ix_alarm_action_target ON alarm_action(wellhead_id, alarm_rule_id, occurred_at DESC, action_id DESC);

CREATE TABLE alarm_acknowledgement (
    event_id BIGINT NOT NULL CHECK (event_id > 0),
    triggered_at TIMESTAMPTZ NOT NULL CHECK (isfinite(triggered_at)),
    acknowledged_at TIMESTAMPTZ NOT NULL CHECK (isfinite(acknowledged_at)),
    action_id UUID NOT NULL UNIQUE REFERENCES alarm_action(action_id),
    PRIMARY KEY (event_id, triggered_at)
);

CREATE TABLE alarm_shelf (
    wellhead_id INT NOT NULL REFERENCES wellhead(wellhead_id) ON DELETE CASCADE,
    alarm_rule_id INT NOT NULL REFERENCES alarmrule(alarm_rule_id) ON DELETE CASCADE,
    revision INT NOT NULL CHECK (revision > 0),
    shelved_until TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL CHECK (isfinite(updated_at)),
    action_id UUID NOT NULL REFERENCES alarm_action(action_id),
    PRIMARY KEY (wellhead_id, alarm_rule_id),
    CHECK (shelved_until IS NULL OR (isfinite(shelved_until) AND
        shelved_until >= updated_at + INTERVAL '5 minutes' AND shelved_until <= updated_at + INTERVAL '8 hours'))
);
CREATE INDEX ix_alarm_shelf_deadline ON alarm_shelf(shelved_until) WHERE shelved_until IS NOT NULL;
