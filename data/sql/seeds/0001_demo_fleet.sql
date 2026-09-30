-- Demo fleet: 1 field, 2 pads, 12 wellheads, 18 parameters, and the alarm
-- rules that make the dashboard show something.
--
-- Separated from the schema migrations because this is illustrative fixture
-- data, not structure. A deployment with real assets applies the migrations
-- and skips this file.

-- 1. Create Fields and Locations
INSERT INTO field (field_id, name, description)
VALUES (1, 'North Field', 'Primary production field')
ON CONFLICT (field_id) DO NOTHING;

INSERT INTO location (location_id, field_id, name, latitude, longitude) VALUES
(1, 1, 'Pad A', 60.123, -97.456),
(2, 1, 'Pad B', 60.125, -97.458)
ON CONFLICT (location_id) DO NOTHING;

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
(18, 'pump_status', 'Pump Status', 'state', 'boolean', 0, 1)
ON CONFLICT (parameter_type_id) DO NOTHING;

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
        INSERT INTO device (device_id, name, modbus_unit_id)
        VALUES (wellhead_index, 'WH-' || LPAD(wellhead_index::text, 3, '0') || '-RTU', 1)
        ON CONFLICT (device_id) DO NOTHING;

        INSERT INTO wellHead (wellhead_id, location_id, device_id, name, type)
        VALUES (wellhead_index, current_location_id, wellhead_index, 'WH-' || LPAD(wellhead_index::text, 3, '0'), 'Oil Producer')
        ON CONFLICT (wellhead_id) DO NOTHING;

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
            )
            ON CONFLICT (device_id, modbus_register) DO NOTHING;
        END LOOP;
    END LOOP;
END $$;

SELECT setval(pg_get_serial_sequence('field', 'field_id'), (SELECT MAX(field_id) FROM field));
SELECT setval(pg_get_serial_sequence('location', 'location_id'), (SELECT MAX(location_id) FROM location));
SELECT setval(pg_get_serial_sequence('device', 'device_id'), (SELECT MAX(device_id) FROM device));
SELECT setval(pg_get_serial_sequence('wellhead', 'wellhead_id'), (SELECT MAX(wellhead_id) FROM wellhead));
SELECT setval(pg_get_serial_sequence('parametertype', 'parameter_type_id'), (SELECT MAX(parameter_type_id) FROM parameterType));


-- 4. Create a comprehensive set of Alarm Rules
INSERT INTO alarmRule (parameter_type_id, severity_level, operator, threshold_value) VALUES
(1, 'CRITICAL', '>', 3000), -- High Tubing Pressure
(1, 'WARNING', '<', 500),   -- Low Tubing Pressure
(4, 'CRITICAL', '>', 250),  -- High Wellhead Temp
(6, 'WARNING', '>', 4800),  -- High Flow Rate
(8, 'CRITICAL', '>', 75),   -- High Water Cut
(11, 'CRITICAL', '>', 40),  -- High H2S Level
(13, 'WARNING', '>', 4.5)   -- High Vibration
ON CONFLICT DO NOTHING;
