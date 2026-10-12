-- migrate:no-transaction
-- Milestone 7: append-only model snapshots, recovery events, and audited
-- per-well parameter overrides. Policy functions run as separate statements.

CREATE TABLE IF NOT EXISTS twin_state_snapshot (
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
CREATE INDEX IF NOT EXISTS ix_twin_state_snapshot_well_time
    ON twin_state_snapshot (wellhead_id, simulated_at DESC);
ALTER TABLE twin_state_snapshot SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'wellhead_id',
    timescaledb.compress_orderby = 'simulated_at DESC'
);
SELECT add_compression_policy('twin_state_snapshot', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('twin_state_snapshot', INTERVAL '90 days', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS twin_recovery_event (
    event_id BIGSERIAL NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    wellhead_id INT REFERENCES wellhead(wellhead_id),
    event_type TEXT NOT NULL CHECK (event_type IN (
        'snapshot_restored', 'cold_start_no_snapshot', 'snapshot_incompatible',
        'clock_jump_forward', 'clock_jump_backward'
    )),
    model_version model_version_text NOT NULL,
    snapshot_simulated_at TIMESTAMPTZ,
    gap_seconds DOUBLE PRECISION CHECK (gap_seconds IS NULL OR gap_seconds >= 0),
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (event_id, occurred_at)
);
SELECT create_hypertable('twin_recovery_event', 'occurred_at', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS ix_twin_recovery_event_well_time
    ON twin_recovery_event (wellhead_id, occurred_at DESC);
SELECT add_retention_policy('twin_recovery_event', INTERVAL '2 years', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS wellhead_model_parameter (
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
CREATE INDEX IF NOT EXISTS ix_wellhead_model_parameter_current
    ON wellhead_model_parameter (wellhead_id, model_version, parameter_key, valid_from DESC);
