-- Drop existing views/materialized views if they exist to allow re-running this script
DROP MATERIALIZED VIEW IF EXISTS mv_hourly_pressure_trends CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_hourly_temp_flow_trends CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_hourly_water_cut_gor_trends CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_daily_all_parameter_summary CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_daily_alarm_counts CASCADE;
DROP VIEW IF EXISTS v_wellhead_parameter_readings CASCADE;
DROP VIEW IF EXISTS v_active_alarms CASCADE;


-- ================================================================
-- VIEWS FOR WELLHEAD DATA ANALYSIS
-- =================================================================

-- 1. Base Parameter Readings View (Denormalized for easier querying)
-- This view joins parameter readings with all relevant metadata,
-- making it easier for applications to display and filter data
-- without complex joins in every query.
CREATE OR REPLACE VIEW v_wellhead_parameter_readings AS
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
FROM
    parameterReading pr
JOIN
    wellHead wh ON pr.wellhead_id = wh.wellhead_id
JOIN
    location loc ON wh.location_id = loc.location_id
JOIN
    field f ON loc.field_id = f.field_id
JOIN
    parameterType pt ON pr.parameter_type_id = pt.parameter_type_id;

-- ================================================================
-- CONTINUOUS AGGREGATES (MATERIALIZED VIEWS FOR TRENDS)
-- These views pre-calculate common aggregations to speed up
-- frequently requested trend analyses.
-- =================================================================

-- 2. Hourly Pressure Trends (Tubing, Casing, Annulus)
-- Provides hourly average, min, and max for key pressure parameters.

CREATE MATERIALIZED VIEW mv_hourly_pressure_trends
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', timestamp_utc) AS bucket_time,
    wellhead_id,
    wellhead_name,
    location_name,
    field_name,
    parameter_code,
    parameter_display_name,
    canonical_unit,
    AVG(raw_value) AS avg_value,
    MIN(raw_value) AS min_value,
    MAX(raw_value) AS max_value,
    COUNT(raw_value) AS reading_count
FROM
    v_wellhead_parameter_readings
WHERE
    parameter_code IN ('tubing_pressure', 'casing_pressure', 'annulus_pressure')
GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8;

-- Create a refresh policy for hourly pressure trends (e.g., refresh every 30 minutes, keeping 1 day of raw data)
SELECT add_continuous_aggregate_policy('mv_hourly_pressure_trends',
  start_offset => INTERVAL '1 day',
  end_offset => INTERVAL '1 hour',
  schedule_interval => INTERVAL '30 minutes');


-- 3. Hourly Temperature and Flow Rate Trends
-- Provides hourly average, min, and max for temperature and flow rate.

CREATE MATERIALIZED VIEW mv_hourly_temp_flow_trends
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', timestamp_utc) AS bucket_time,
    wellhead_id,
    wellhead_name,
    location_name,
    field_name,
    parameter_code,
    parameter_display_name,
    canonical_unit,
    AVG(raw_value) AS avg_value,
    MIN(raw_value) AS min_value,
    MAX(raw_value) AS max_value,
    COUNT(raw_value) AS reading_count
FROM
    v_wellhead_parameter_readings
WHERE
    parameter_code IN ('wellhead_temperature', 'flow_rate')
GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8;

-- Create a refresh policy for hourly temp/flow trends

SELECT add_continuous_aggregate_policy('mv_hourly_temp_flow_trends',
  start_offset => INTERVAL '1 day',
  end_offset => INTERVAL '1 hour',
  schedule_interval => INTERVAL '30 minutes');


-- 4. Hourly Water Cut and Gas Oil Ratio Trends
-- Provides hourly average, min, and max for water cut and GOR.

CREATE MATERIALIZED VIEW mv_hourly_water_cut_gor_trends
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', timestamp_utc) AS bucket_time,
    wellhead_id,
    wellhead_name,
    location_name,
    field_name,
    parameter_code,
    parameter_display_name,
    canonical_unit,
    AVG(raw_value) AS avg_value,
    MIN(raw_value) AS min_value,
    MAX(raw_value) AS max_value,
    COUNT(raw_value) AS reading_count
FROM
    v_wellhead_parameter_readings
WHERE
    parameter_code IN ('water_cut', 'gas_oil_ratio')
GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8;

-- Create a refresh policy for hourly water cut/GOR trends
SELECT add_continuous_aggregate_policy('mv_hourly_water_cut_gor_trends',
  start_offset => INTERVAL '1 day',
  end_offset => INTERVAL '1 hour',
  schedule_interval => INTERVAL '30 minutes');


-- 5. Daily Parameter Summary for ALL Numerical Parameters
-- Provides daily average, min, max, and standard deviation for all numerical parameters.

CREATE MATERIALIZED VIEW mv_daily_all_parameter_summary
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', timestamp_utc) AS bucket_day,
    wellhead_id,
    wellhead_name,
    location_name,
    field_name,
    parameter_code,
    parameter_display_name,
    canonical_unit,
    AVG(raw_value) AS daily_avg_value,
    MIN(raw_value) AS daily_min_value,
    MAX(raw_value) AS daily_max_value,
    STDDEV(raw_value) AS daily_stddev_value,
    COUNT(raw_value) AS reading_count
FROM
    v_wellhead_parameter_readings
WHERE
    data_type IN ('float', 'integer') -- Exclude boolean parameters from numerical stats
GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8;

-- Create a refresh policy for daily parameter summary (e.g., refresh once a day, keeping 7 days of raw data)
SELECT add_continuous_aggregate_policy('mv_daily_all_parameter_summary',
  start_offset => INTERVAL '7 days',
  end_offset => INTERVAL '1 day',
  schedule_interval => INTERVAL '1 day');


-- 6. Active Alarms View
-- Shows all alarms that are currently active (cleared_at IS NULL).

CREATE OR REPLACE VIEW v_active_alarms AS
SELECT
    ae.event_id,
    ae.triggered_at,
    ae.severity_level,
    ae.triggered_value,
    wh.wellhead_id,
    wh.name AS wellhead_name,
    loc.name AS location_name,
    f.name AS field_name,
    pt.display_name AS parameter_display_name,
    ar.operator,
    ar.threshold_value,
    ar.alarm_rule_id
FROM
    alarmEvent ae
JOIN
    wellHead wh ON ae.wellhead_id = wh.wellhead_id
JOIN
    location loc ON wh.location_id = loc.location_id
JOIN
    field f ON loc.field_id = f.field_id
JOIN
    alarmRule ar ON ae.alarm_rule_id = ar.alarm_rule_id
JOIN
    parameterType pt ON ar.parameter_type_id = pt.parameter_type_id
WHERE
    ae.cleared_at IS NULL;


-- 7. Daily Alarm Event Counts
-- Provides a daily count of triggered alarms, broken down by wellhead, parameter, and severity.

CREATE MATERIALIZED VIEW mv_daily_alarm_counts
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', ae.triggered_at) AS bucket_day,
    ae.wellhead_id,
    wh.name AS wellhead_name,
    loc.name AS location_name,
    pt.display_name AS parameter_display_name,
    ae.severity_level,
    COUNT(ae.event_id) AS total_alarms_triggered
FROM
    alarmEvent ae
JOIN
    wellHead wh ON ae.wellhead_id = wh.wellhead_id
JOIN
    location loc ON wh.location_id = loc.location_id
JOIN
    parameterType pt ON ae.parameter_type_id = pt.parameter_type_id -- Note: parameter_type_id is in alarmEvent
GROUP BY
    1, 2, 3, 4, 5, 6;

-- Create a refresh policy for daily alarm counts
SELECT add_continuous_aggregate_policy('mv_daily_alarm_counts',
  start_offset => INTERVAL '30 days',
  end_offset => INTERVAL '1 day',
  schedule_interval => INTERVAL '1 day');