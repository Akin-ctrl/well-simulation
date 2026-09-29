-- One indexed lookup per active mapping keeps the twin-core's latest-reading
-- endpoint bounded as historian data grows.
CREATE INDEX IF NOT EXISTS ix_parameter_reading_mapping_time
    ON parameterReading (mapping_id, timestamp_utc DESC, parameter_reading_id DESC);
