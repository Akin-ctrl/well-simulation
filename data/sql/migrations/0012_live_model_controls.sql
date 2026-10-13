-- Durable synthetic control commands. No field-device write path is added.
CREATE TABLE IF NOT EXISTS control_command (
    command_id UUID PRIMARY KEY,
    wellhead_id INT NOT NULL REFERENCES wellhead(wellhead_id),
    created_by INT NOT NULL REFERENCES users(id),
    idempotency_key UUID NOT NULL,
    reason VARCHAR(500) NOT NULL CHECK (length(trim(reason)) > 0),
    expected_revision INT NOT NULL CHECK (expected_revision >= 0),
    model_version model_version_text NOT NULL,
    parameter_fingerprint TEXT NOT NULL,
    choke_position_percent DOUBLE PRECISION CHECK (choke_position_percent BETWEEN 0 AND 100),
    master_valve_open BOOLEAN,
    wing_valve_open BOOLEAN,
    pump_on BOOLEAN,
    emergency_shutdown BOOLEAN,
    status TEXT NOT NULL DEFAULT 'draft' CHECK (status IN
        ('draft', 'queued', 'applied', 'rejected', 'expired', 'cancelled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    expires_at TIMESTAMPTZ NOT NULL DEFAULT (clock_timestamp() + INTERVAL '24 hours'),
    queued_at TIMESTAMPTZ,
    activated_by INT REFERENCES users(id),
    activated_role TEXT,
    activated_session_version INT,
    result_at TIMESTAMPTZ,
    result_revision INT,
    result_reason VARCHAR(64),
    request_id VARCHAR(64),
    UNIQUE (created_by, idempotency_key),
    CHECK (num_nonnulls(choke_position_percent, master_valve_open,
        wing_valve_open, pump_on, emergency_shutdown) > 0),
    CHECK (emergency_shutdown IS NULL OR num_nonnulls(choke_position_percent,
        master_valve_open, wing_valve_open, pump_on) = 0),
    CHECK (status <> 'queued' OR (queued_at IS NOT NULL AND activated_by IS NOT NULL AND activated_by = created_by
        AND activated_role IS NOT NULL AND activated_session_version IS NOT NULL)),
    CHECK ((status IN ('applied', 'rejected', 'expired', 'cancelled')) = (result_at IS NOT NULL)),
    CHECK (status <> 'applied' OR (result_revision IS NOT NULL AND result_revision = expected_revision + 1))
);
CREATE UNIQUE INDEX ix_control_one_draft
    ON control_command(created_by, wellhead_id) WHERE status = 'draft';
CREATE UNIQUE INDEX ix_control_one_queued
    ON control_command(wellhead_id) WHERE status = 'queued';
CREATE INDEX ix_control_inbox ON control_command(queued_at, command_id)
    WHERE status = 'queued';

CREATE TABLE IF NOT EXISTS wellhead_control_state (
    wellhead_id INT PRIMARY KEY REFERENCES wellhead(wellhead_id),
    revision INT NOT NULL DEFAULT 0 CHECK (revision >= 0),
    model_version model_version_text NOT NULL,
    parameter_fingerprint TEXT NOT NULL,
    choke_position_percent DOUBLE PRECISION NOT NULL CHECK (choke_position_percent BETWEEN 0 AND 100),
    master_valve_open BOOLEAN NOT NULL,
    wing_valve_open BOOLEAN NOT NULL,
    swab_valve_open BOOLEAN NOT NULL,
    pump_on BOOLEAN NOT NULL,
    emergency_shutdown BOOLEAN NOT NULL,
    applied_command_id UUID REFERENCES control_command(command_id),
    applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS control_command_event (
    event_id BIGSERIAL PRIMARY KEY,
    command_id UUID NOT NULL REFERENCES control_command(command_id),
    event_type VARCHAR(32) NOT NULL CHECK (event_type IN
        ('draft', 'queued', 'applied', 'rejected', 'expired', 'cancelled')),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    actor_user_id INT REFERENCES users(id),
    reason_code VARCHAR(64) NOT NULL,
    request_id VARCHAR(64)
);
CREATE INDEX ix_control_event_command ON control_command_event(command_id, event_id);

CREATE TABLE IF NOT EXISTS model_parameter_change (
    change_id UUID PRIMARY KEY,
    wellhead_id INT NOT NULL REFERENCES wellhead(wellhead_id),
    created_by INT NOT NULL REFERENCES users(id),
    idempotency_key UUID NOT NULL,
    model_version model_version_text NOT NULL,
    parameter_key TEXT NOT NULL,
    value DOUBLE PRECISION,
    unit TEXT,
    reason VARCHAR(500) NOT NULL CHECK (length(trim(reason)) > 0),
    expected_override_id INT REFERENCES wellhead_model_parameter(parameter_override_id),
    status TEXT NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'published', 'cancelled', 'expired')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    expires_at TIMESTAMPTZ NOT NULL DEFAULT (clock_timestamp() + INTERVAL '24 hours'),
    result_at TIMESTAMPTZ,
    published_override_id INT REFERENCES wellhead_model_parameter(parameter_override_id),
    UNIQUE (created_by, idempotency_key),
    CHECK ((value IS NULL) = (unit IS NULL)),
    CHECK (value IS NULL OR (value > '-Infinity'::float8 AND value < 'Infinity'::float8)),
    CHECK ((status <> 'draft') = (result_at IS NOT NULL)),
    CHECK ((status = 'published') = (published_override_id IS NOT NULL))
);
CREATE UNIQUE INDEX ix_parameter_one_draft
    ON model_parameter_change(created_by, wellhead_id, parameter_key) WHERE status = 'draft';

-- Narrow lock helpers avoid granting twin-core user-update rights or granting
-- the API active-control-update rights merely to acquire a row lock.
CREATE FUNCTION control_actor_authority(actor_id INT)
RETURNS TABLE(role TEXT, status TEXT, deleted BOOLEAN, session_version INT)
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp
AS $$ SELECT role::text, status::text, deleted, session_version
      FROM public.users WHERE id = actor_id FOR SHARE $$;
REVOKE ALL ON FUNCTION control_actor_authority(INT) FROM PUBLIC;

CREATE FUNCTION lock_control_state(well_id INT) RETURNS VOID
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp
AS $$ SELECT 1 FROM public.wellhead_control_state WHERE wellhead_id = well_id FOR UPDATE $$;
REVOKE ALL ON FUNCTION lock_control_state(INT) FROM PUBLIC;

CREATE FUNCTION enforce_control_lifecycle() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, public, pg_temp AS $$
BEGIN
    IF (to_jsonb(NEW) - ARRAY['status','expires_at','queued_at','activated_by',
        'activated_role','activated_session_version','result_at','result_revision','result_reason'])
        IS DISTINCT FROM
       (to_jsonb(OLD) - ARRAY['status','expires_at','queued_at','activated_by',
        'activated_role','activated_session_version','result_at','result_revision','result_reason']) THEN
        RAISE EXCEPTION 'Control proposals are immutable';
    END IF;
    IF NOT ((OLD.status = 'draft' AND NEW.status IN ('queued','cancelled','expired'))
        OR (OLD.status = 'queued' AND NEW.status IN ('applied','rejected','expired','cancelled'))) THEN
        RAISE EXCEPTION 'Invalid control lifecycle transition';
    END IF;
    IF OLD.status = 'queued' AND
       ROW(NEW.queued_at, NEW.expires_at, NEW.activated_by, NEW.activated_role, NEW.activated_session_version)
       IS DISTINCT FROM
       ROW(OLD.queued_at, OLD.expires_at, OLD.activated_by, OLD.activated_role, OLD.activated_session_version) THEN
        RAISE EXCEPTION 'Activation authority is immutable';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER enforce_control_lifecycle BEFORE UPDATE ON control_command
    FOR EACH ROW EXECUTE FUNCTION enforce_control_lifecycle();

CREATE FUNCTION enforce_parameter_change_lifecycle() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, public, pg_temp AS $$
BEGIN
    IF (to_jsonb(NEW) - ARRAY['status','result_at','published_override_id'])
       IS DISTINCT FROM (to_jsonb(OLD) - ARRAY['status','result_at','published_override_id'])
       OR OLD.status <> 'draft' OR NEW.status NOT IN ('published','cancelled','expired') THEN
        RAISE EXCEPTION 'Invalid parameter change lifecycle transition';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER enforce_parameter_change_lifecycle BEFORE UPDATE ON model_parameter_change
    FOR EACH ROW EXECUTE FUNCTION enforce_parameter_change_lifecycle();

ALTER TABLE twin_recovery_event DROP CONSTRAINT twin_recovery_event_event_type_check;
ALTER TABLE twin_recovery_event ADD CHECK (event_type IN (
    'snapshot_restored', 'cold_start_no_snapshot', 'snapshot_incompatible',
    'clock_jump_forward', 'clock_jump_backward', 'active_controls_restored'
));
