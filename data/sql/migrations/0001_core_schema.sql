-- Core relational schema, hypertables, and indexes.
--
-- Idempotent: safe to apply to a database originally created by the previous
-- docker-entrypoint-initdb.d bootstrap, and to a fresh one.

CREATE EXTENSION IF NOT EXISTS timescaledb;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'status') THEN
        CREATE TYPE "status" AS ENUM ('ACTIVE', 'DISABLED');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'role') THEN
        CREATE TYPE "role" AS ENUM ('USER', 'ADMIN', 'OPERATIONS');
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS users (
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
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Asset hierarchy -----------------------------------------------------------

CREATE TABLE IF NOT EXISTS field (
    field_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT
);

CREATE TABLE IF NOT EXISTS location (
    location_id SERIAL PRIMARY KEY,
    field_id INT NOT NULL REFERENCES field(field_id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    address TEXT,
    latitude FLOAT,
    longitude FLOAT
);

CREATE TABLE IF NOT EXISTS device (
    device_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    modbus_unit_id INT NOT NULL,
    status VARCHAR(50) DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS wellHead (
    wellhead_id SERIAL PRIMARY KEY,
    location_id INT NOT NULL REFERENCES location(location_id) ON DELETE CASCADE,
    device_id INT UNIQUE NOT NULL REFERENCES device(device_id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL UNIQUE,
    type VARCHAR(100),
    status VARCHAR(50) DEFAULT 'active'
);

-- Parameter metadata --------------------------------------------------------

CREATE TABLE IF NOT EXISTS parameterType (
    parameter_type_id SERIAL PRIMARY KEY,
    code VARCHAR(100) NOT NULL UNIQUE,
    display_name VARCHAR(255) NOT NULL,
    canonical_unit VARCHAR(50),
    data_type VARCHAR(50) NOT NULL,
    normal_min FLOAT,
    normal_max FLOAT
);

CREATE TABLE IF NOT EXISTS deviceParameterMapping (
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

CREATE TABLE IF NOT EXISTS parameterReading (
    parameter_reading_id BIGSERIAL,
    timestamp_utc TIMESTAMPTZ NOT NULL,
    wellhead_id INT NOT NULL REFERENCES wellHead(wellhead_id) ON DELETE CASCADE,
    parameter_type_id INT NOT NULL REFERENCES parameterType(parameter_type_id) ON DELETE CASCADE,
    mapping_id INT NOT NULL REFERENCES deviceParameterMapping(mapping_id) ON DELETE CASCADE,
    raw_value FLOAT NOT NULL,
    inserted_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (parameter_reading_id, timestamp_utc)
);

SELECT create_hypertable('parameterReading', 'timestamp_utc', if_not_exists => TRUE);

-- Every per-wellhead query filters on exactly this pair; no index covered it.
CREATE INDEX IF NOT EXISTS ix_parameter_reading_wellhead_time
    ON parameterReading (wellhead_id, timestamp_utc DESC);

CREATE INDEX IF NOT EXISTS ix_parameter_reading_type_time
    ON parameterReading (parameter_type_id, timestamp_utc DESC);

-- Alarms --------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS alarmRule (
    alarm_rule_id SERIAL PRIMARY KEY,
    parameter_type_id INT NOT NULL REFERENCES parameterType(parameter_type_id) ON DELETE CASCADE,
    severity_level VARCHAR(50) NOT NULL,
    operator VARCHAR(10) NOT NULL,
    threshold_value FLOAT NOT NULL,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_alarm_rule_definition
    ON alarmRule (parameter_type_id, severity_level, operator, threshold_value);

CREATE TABLE IF NOT EXISTS alarmEvent (
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
CREATE INDEX IF NOT EXISTS ix_alarm_event_open
    ON alarmEvent (wellhead_id, alarm_rule_id) WHERE cleared_at IS NULL;
