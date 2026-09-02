-- migrate:no-transaction
--
-- The audit trail, implementing ADR 0034.
--
-- Nothing in this system recorded who did anything. This table answers who did
-- what, when, and to which record. It is built for the person operating the
-- system rather than for a regulator, so there is no hash chain and no separate
-- store. ADR 0034 records why.

CREATE TABLE IF NOT EXISTS auditEvent (
    audit_event_id BIGSERIAL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Null when no known user acted, which is every failed login against an
    -- account that does not exist.
    actor_user_id INT REFERENCES users(id) ON DELETE SET NULL,

    -- What the caller submitted. Recorded whether or not it matches an account,
    -- so the presence of a row cannot be used to discover which accounts exist.
    -- Never populated from the password field.
    actor_identifier VARCHAR(256),

    action VARCHAR(64) NOT NULL,
    subject_type VARCHAR(64),
    subject_id VARCHAR(64),
    outcome VARCHAR(16) NOT NULL,

    -- Ties the row to one request and to the log lines that request produced.
    request_id VARCHAR(64),

    detail JSONB NOT NULL DEFAULT '{}'::jsonb,

    PRIMARY KEY (audit_event_id, occurred_at),
    CONSTRAINT audit_event_outcome CHECK (outcome IN ('success', 'failure'))
);

SELECT create_hypertable('auditEvent', 'occurred_at', if_not_exists => TRUE);

-- Reading the trail means asking what one actor did, or what happened to one
-- subject, almost always most recent first.
CREATE INDEX IF NOT EXISTS ix_audit_event_actor
    ON auditEvent (actor_user_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS ix_audit_event_action
    ON auditEvent (action, occurred_at DESC);
CREATE INDEX IF NOT EXISTS ix_audit_event_subject
    ON auditEvent (subject_type, subject_id, occurred_at DESC);

-- Failed logins arrive from unauthenticated traffic, so the identifier is the
-- column an investigation starts from.
CREATE INDEX IF NOT EXISTS ix_audit_event_identifier
    ON auditEvent (actor_identifier, occurred_at DESC)
    WHERE actor_identifier IS NOT NULL;

SELECT add_retention_policy('auditEvent', INTERVAL '90 days',
    if_not_exists => TRUE);
