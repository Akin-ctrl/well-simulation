-- migrate:no-transaction
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
