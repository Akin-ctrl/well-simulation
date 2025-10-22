-- =================================================================
-- SCHEMA DEFINITION
-- This script creates the entire database schema based on the provided data model.
-- =================================================================

-- Enable necessary extensions
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Asset Metadata
CREATE TABLE field (
    field_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT
);

CREATE TABLE location (
    location_id SERIAL PRIMARY KEY,
    field_id INT NOT NULL REFERENCES field(field_id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    address TEXT,
    latitude FLOAT,
    longitude FLOAT
);

CREATE TABLE device (
    device_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    modbus_unit_id INT NOT NULL,
    status VARCHAR(50) DEFAULT 'active'
);

CREATE TABLE wellHead (
    wellhead_id SERIAL PRIMARY KEY,
    location_id INT NOT NULL REFERENCES location(location_id) ON DELETE CASCADE,
    device_id INT UNIQUE NOT NULL REFERENCES device(device_id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL UNIQUE,
    type VARCHAR(100),
    status VARCHAR(50) DEFAULT 'active'
);

-- Parameter Metadata
CREATE TABLE parameterType (
    parameter_type_id SERIAL PRIMARY KEY,
    code VARCHAR(100) NOT NULL UNIQUE, -- e.g., 'tubing_pressure'
    display_name VARCHAR(255) NOT NULL,
    canonical_unit VARCHAR(50),
    data_type VARCHAR(50) NOT NULL, -- 'float', 'integer', 'boolean'
    normal_min FLOAT,
    normal_max FLOAT
);

CREATE TABLE deviceParameterMapping (
    mapping_id SERIAL PRIMARY KEY,
    device_id INT NOT NULL REFERENCES device(device_id) ON DELETE CASCADE,
    parameter_type_id INT NOT NULL REFERENCES parameterType(parameter_type_id) ON DELETE CASCADE,
    modbus_register INT NOT NULL,
    function_code SMALLINT NOT NULL, -- e.g., 3 for holding register
    register_type VARCHAR(50) NOT NULL,
    active BOOLEAN DEFAULT TRUE,
    UNIQUE(device_id, modbus_register)
);

-- Time-Series Data (Hypertables)
CREATE TABLE parameterReading (
    parameter_reading_id BIGSERIAL,
    timestamp_utc TIMESTAMPTZ NOT NULL,
    wellhead_id INT NOT NULL REFERENCES wellHead(wellhead_id) ON DELETE CASCADE,
    parameter_type_id INT NOT NULL REFERENCES parameterType(parameter_type_id) ON DELETE CASCADE,
    mapping_id INT NOT NULL REFERENCES deviceParameterMapping(mapping_id) ON DELETE CASCADE,
    raw_value FLOAT NOT NULL,
    PRIMARY KEY (parameter_reading_id, timestamp_utc),
    inserted_at TIMESTAMPTZ DEFAULT NOW()
);

SELECT create_hypertable('parameterReading', 'timestamp_utc');

-- Derived Data (Alarms)
CREATE TABLE alarmRule (
    alarm_rule_id SERIAL PRIMARY KEY,
    parameter_type_id INT NOT NULL REFERENCES parameterType(parameter_type_id) ON DELETE CASCADE,
    severity_level VARCHAR(50) NOT NULL, -- e.g., 'CRITICAL', 'WARNING'
    operator VARCHAR(10) NOT NULL, -- e.g., '>', '<', '=='
    threshold_value FLOAT NOT NULL,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);


CREATE TABLE alarmEvent (
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

SELECT create_hypertable('alarmEvent', 'triggered_at');


-- =================================================================
-- SEED METADATA (CRITICAL FOR THE SYSTEM TO RUN)
-- This section populates the tables with the configuration for 12 wellheads and 18 parameters.
-- =================================================================

-- 1. Create Fields and Locations
INSERT INTO field (name, description) VALUES ('North Field', 'Primary production field');
INSERT INTO location (field_id, name, latitude, longitude) VALUES 
(1, 'Pad A', 60.123, -97.456),
(1, 'Pad B', 60.125, -97.458);

-- 2. Create All 18 Parameter Types
INSERT INTO parameterType (parameter_type_id, code, display_name, canonical_unit, data_type, normal_min, normal_max) VALUES
(1, 'tubing_pressure', 'Tubing Pressure', 'psi', 'float', 500, 3000),
(2, 'casing_pressure', 'Casing Pressure', 'psi', 'float', 200, 2000),
(3, 'annulus_pressure', 'Annulus Pressure', 'psi', 'float', 50, 500),
(4, 'wellhead_temperature', 'Wellhead Temperature', '°F', 'float', 60, 250),
(5, 'choke_valve_position', 'Choke Valve Position', '%', 'integer', 0, 100),
(6, 'flow_rate', 'Flow Rate', 'bbl/day', 'float', 100, 5000),
(7, 'gas_oil_ratio', 'Gas Oil Ratio', 'scf/stb', 'float', 500, 3000),
(8, 'water_cut', 'Water Cut', '%', 'float', 0, 80),
(9, 'sand_detector', 'Sand Detector', 'ppm', 'float', 0, 100),
(10, 'corrosion_rate', 'Corrosion Rate', 'mpy', 'float', 0, 10),
(11, 'h2s_level', 'H2S Level', 'ppm', 'float', 0, 50),
(12, 'co2_level', 'CO2 Level', '%', 'float', 0, 5),
(13, 'vibration', 'Vibration', 'mm/s', 'float', 0, 5),
(14, 'master_valve_status', 'Master Valve Status', 'state', 'boolean', 0, 1),
(15, 'wing_valve_status', 'Wing Valve Status', 'state', 'boolean', 0, 1),
(16, 'swab_valve_status', 'Swab Valve Status', 'state', 'boolean', 0, 1),
(17, 'emergency_shutdown', 'Emergency Shutdown', 'state', 'boolean', 0, 1),
(18, 'pump_status', 'Pump Status', 'state', 'boolean', 0, 1);

-- 3. Create 12 Wellheads and their associated Devices and Mappings
-- We use a DO block to programmatically create the assets and mappings.
DO $$
DECLARE
    wellhead_index INT;
    param_index INT;
    base_register INT;
    current_location_id INT;
BEGIN
    FOR wellhead_index IN 1..12 LOOP
        -- Alternate wellheads between Pad A (location_id=1) and Pad B (location_id=2)
        IF wellhead_index <= 6 THEN
            current_location_id := 1;
        ELSE
            current_location_id := 2;
        END IF;

        -- Create Device and Wellhead
        INSERT INTO device (device_id, name, modbus_unit_id) VALUES (wellhead_index, 'WH-' || LPAD(wellhead_index::text, 3, '0') || '-RTU', 1);
        INSERT INTO wellHead (wellhead_id, location_id, device_id, name, type) VALUES (wellhead_index, current_location_id, wellhead_index, 'WH-' || LPAD(wellhead_index::text, 3, '0'), 'Oil Producer');

        -- Define the base Modbus register for this wellhead to avoid overlap
        -- We allocate 100 registers per wellhead for ample space.
        base_register := (wellhead_index - 1) * 100;

        -- Create Mappings for all 18 parameters for this wellhead
        FOR param_index IN 1..18 LOOP
            INSERT INTO deviceParameterMapping (device_id, parameter_type_id, modbus_register, function_code, register_type)
            VALUES (
                wellhead_index, -- device_id
                param_index,    -- parameter_type_id
                base_register + ((param_index - 1) * 2), -- Each parameter takes 2 registers (32-bit)
                3,              -- Modbus function code 3 (Read Holding Registers)
                'holding'       -- Register type
            );
        END LOOP;
    END LOOP;
END $$;


-- 4. Create a comprehensive set of Alarm Rules
INSERT INTO alarmRule (parameter_type_id, severity_level, operator, threshold_value) VALUES
(1, 'CRITICAL', '>', 3000), -- High Tubing Pressure
(1, 'WARNING', '<', 500),   -- Low Tubing Pressure
(4, 'CRITICAL', '>', 250),  -- High Wellhead Temp
(6, 'WARNING', '>', 4800),  -- High Flow Rate
(8, 'CRITICAL', '>', 75),   -- High Water Cut
(11, 'CRITICAL', '>', 40),  -- High H2S Level
(13, 'WARNING', '>', 4.5);  -- High Vibration


-- =================================================================
-- DATABASE TRIGGER FOR REAL-TIME ALARM CHECKING
-- This trigger function evaluates every new reading against the alarm rules.
-- =================================================================

CREATE OR REPLACE FUNCTION check_wellhead_alarms()
RETURNS TRIGGER AS $$
DECLARE
    rule record;
    is_active BOOLEAN;
BEGIN
    -- Loop through all active rules for the parameter type of the new reading
    FOR rule IN SELECT * FROM alarmRule WHERE parameter_type_id = NEW.parameter_type_id AND active = TRUE
    LOOP
        -- Check if an alarm for this rule and wellhead is already active
        SELECT EXISTS(
            SELECT 1 FROM alarmEvent
            WHERE wellhead_id = NEW.wellhead_id
            AND alarm_rule_id = rule.alarm_rule_id
            AND cleared_at IS NULL
        ) INTO is_active;

        -- Evaluate the rule condition
        IF (rule.operator = '>' AND NEW.raw_value > rule.threshold_value) OR
           (rule.operator = '<' AND NEW.raw_value < rule.threshold_value) OR
           (rule.operator = '==' AND NEW.raw_value = rule.threshold_value) THEN
            
            -- If condition is met and no alarm is active, create a new alarm event
            IF NOT is_active THEN
                INSERT INTO alarmEvent (alarm_rule_id, parameter_reading_id, wellhead_id, timestamp_utc, triggered_at, severity_level, triggered_value)
                VALUES (rule.alarm_rule_id, NEW.parameter_reading_id, NEW.wellhead_id, NEW.timestamp_utc, NEW.timestamp_utc, rule.severity_level, NEW.raw_value);
            END IF;
        ELSE
            -- If condition is NOT met but an alarm IS active, clear it
            IF is_active THEN
                UPDATE alarmEvent
                SET cleared_at = NEW.timestamp_utc
                WHERE wellhead_id = NEW.wellhead_id
                AND alarm_rule_id = rule.alarm_rule_id
                AND cleared_at IS NULL;
            END IF;
        END IF;
    END LOOP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Attach the trigger to the readings table
CREATE TRIGGER wellhead_readings_alarm_trigger
AFTER INSERT ON parameterReading
FOR EACH ROW
EXECUTE FUNCTION check_wellhead_alarms();