-- Record which model version produced each model reading (ADR 0036, amendment
-- of 2026-10-02). The gateway publishes the version in the Modbus registers
-- after the heartbeat, and ingestion stores it as text such as '0.3.0'.
--
-- Rows written before this migration keep NULL, which means "not recorded".
-- Their version cannot be recovered truthfully, so it is not backfilled.
--
-- The format rule lives on a domain (a named type with its own check), not on
-- a table CHECK constraint. TimescaleDB refuses any new table constraint, even
-- one marked NOT VALID, once a hypertable holds compressed chunks, and
-- parameterReading compresses readings older than one day. Adding a column of
-- a domain type is allowed, and the domain still rejects a malformed version
-- on every insert. Do not "fix" this into a table constraint: it fails on any
-- volume that has run for more than a day.
--
-- The rule that random simulator rows carry no version needs two columns, so a
-- domain cannot hold it. Ingestion is the only writer and sets the version on
-- every model row it inserts.
CREATE DOMAIN model_version_text AS TEXT
    CHECK (VALUE ~ '^[0-9]+\.[0-9]+\.[0-9]+$');

ALTER TABLE parameterReading
    ADD COLUMN IF NOT EXISTS model_version model_version_text;
