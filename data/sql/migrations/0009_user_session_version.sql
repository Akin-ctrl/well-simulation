-- Let logout end a session on the server (ADR 0037). Each session token
-- carries the user's session_version when it is issued. Logout adds one, and
-- the API rejects any token whose version no longer matches, so a copied
-- cookie stops working on every device at once.
--
-- Existing rows start at 0. Tokens issued before this change carry no version
-- and are rejected, so every user signs in once more after the upgrade.
ALTER TABLE users
    ADD COLUMN IF NOT EXISTS session_version INTEGER NOT NULL DEFAULT 0;
