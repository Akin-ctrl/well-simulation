-- migrate:no-transaction
--
-- Continuous aggregates. TimescaleDB refuses to create these inside a
-- transaction block, so this file is applied with autocommit.
--
-- The previous names described behaviour these views never had: the "hourly"
-- aggregates bucket at 2 minutes and the "daily" ones at 5 minutes, with
-- columns called bucket_day and daily_avg_value. The buckets are deliberately
-- compressed so a demo shows a trend within minutes rather than days; the
-- names now say what they actually do.

DROP MATERIALIZED VIEW IF EXISTS mv_hourly_pressure_trends CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_hourly_temp_flow_trends CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_hourly_water_cut_gor_trends CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_daily_alarm_counts CASCADE;

-- Created, refreshed every five minutes, and read by nothing.
DROP MATERIALIZED VIEW IF EXISTS mv_daily_all_parameter_summary CASCADE;

DROP MATERIALIZED VIEW IF EXISTS mv_2min_pressure_trends CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_2min_temp_flow_trends CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_2min_water_cut_gor_trends CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_5min_alarm_counts CASCADE;

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
