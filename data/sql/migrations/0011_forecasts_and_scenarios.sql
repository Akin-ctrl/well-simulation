-- migrate:no-transaction
-- Milestone 8: complete, attributed model projections. Runs are ordinary
-- tables; sampled future points are hypertables with time in each primary key.

CREATE TABLE IF NOT EXISTS prediction_run (
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
CREATE INDEX IF NOT EXISTS ix_prediction_run_well_created
    ON prediction_run (wellhead_id, created_at DESC);

CREATE TABLE IF NOT EXISTS prediction_point (
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

CREATE TABLE IF NOT EXISTS scenario_run (
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
CREATE INDEX IF NOT EXISTS ix_scenario_run_well_created
    ON scenario_run (wellhead_id, created_at DESC);

CREATE TABLE IF NOT EXISTS scenario_point (
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
