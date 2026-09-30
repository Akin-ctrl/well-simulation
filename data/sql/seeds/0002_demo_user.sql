-- A read-only demo account so the dashboard can be reviewed immediately.
--
-- ADR 0033 closed self-service registration, so without this a fresh checkout
-- would have no way in. The role is USER: it can read the dashboard and run
-- what-if scenarios, and can do nothing else.
--
-- The password is intentionally committed and documented in the README. It
-- grants read access to synthetic telemetry on a local stack and nothing more.
-- The administrator account is NOT seeded here; the migrator generates its
-- password on first run so no privileged credential is ever committed.
--
-- Password: demo-operator-2026

INSERT INTO users (email, user_name, first_name, last_name, role, encrypted_password)
VALUES (
    'demo@example.com',
    'demo',
    'Demo',
    'Reviewer',
    'USER',
    '$2b$11$KAKv9684NpR6U8nHm4v2B.sY6qpkvq40W3ev3V9JL336lwvLqzu0O'
)
ON CONFLICT (email) DO NOTHING;
