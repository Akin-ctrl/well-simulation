-- Denormalised read views for the dashboard API.
--
-- Dropped and recreated rather than CREATE OR REPLACE: replacing a view can
-- only append columns, and v_active_alarms gains parameter_code and
-- canonical_unit in the middle of its column list so the Alarm Center can show
-- the parameter and unit alongside the threshold.

DROP VIEW IF EXISTS v_wellhead_parameter_readings;
DROP VIEW IF EXISTS v_active_alarms;

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
