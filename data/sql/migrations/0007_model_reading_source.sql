-- Keep legacy random readings distinct from model-generated Modbus readings.
ALTER TABLE parameterReading
    ADD COLUMN IF NOT EXISTS source_kind TEXT NOT NULL
    DEFAULT 'synthetic_random_simulator';

ALTER TABLE parameterReading
    ADD CONSTRAINT parameter_reading_source_kind_check
    CHECK (source_kind IN (
        'synthetic_random_simulator', 'synthetic_reduced_order_model'
    ));
