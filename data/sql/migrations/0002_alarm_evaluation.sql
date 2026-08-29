-- Alarm evaluation with hysteresis, evaluated once per insert statement.
--
-- Replaces a per-row AFTER INSERT trigger that looped over rules and ran an
-- EXISTS query for each one: roughly 216 rule evaluations per ingestion batch,
-- inside the insert transaction. It also cleared an alarm on the first in-range
-- reading, so against randomised telemetry alarms flapped continuously and
-- alarmEvent grew without bound from events that lasted a single sample.

-- How many consecutive evaluations must agree before an alarm changes state.
ALTER TABLE alarmRule
    ADD COLUMN IF NOT EXISTS trigger_after INT NOT NULL DEFAULT 3;
ALTER TABLE alarmRule
    ADD COLUMN IF NOT EXISTS clear_after INT NOT NULL DEFAULT 3;

COMMENT ON COLUMN alarmRule.trigger_after IS
    'Consecutive breaching evaluations required before the alarm opens.';
COMMENT ON COLUMN alarmRule.clear_after IS
    'Consecutive non-breaching evaluations required before the alarm clears.';

-- Per wellhead and rule, how many consecutive evaluations have agreed. Keeping
-- the counters here makes evaluation O(1) per rule instead of a scan back
-- through reading history.
CREATE TABLE IF NOT EXISTS alarmRuleState (
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

DROP TRIGGER IF EXISTS wellhead_readings_alarm_trigger ON parameterReading;
DROP TRIGGER IF EXISTS evaluate_wellhead_alarms_trigger ON parameterReading;

CREATE TRIGGER evaluate_wellhead_alarms_trigger
AFTER INSERT ON parameterReading
REFERENCING NEW TABLE AS inserted
FOR EACH STATEMENT
EXECUTE FUNCTION evaluate_wellhead_alarms();

DROP FUNCTION IF EXISTS check_wellhead_alarms();
