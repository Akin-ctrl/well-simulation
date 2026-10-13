# ADR-0037: Logout ends every session of the user, on the server

**Status**: Accepted

**Date**: 2026-10-03

**Decision**: Each user has a session version number. Every session token carries the number it was issued with, and the API refuses a token whose number no longer matches the user's. Logout adds one to the number, so every token issued until then stops working on every device, including a copied cookie. The check costs one primary-key read of the `users` table per authenticated request.

---

## Context

Sessions are signed tokens (JWTs) in an `auth_token` cookie, valid for 24
hours. The API checked only the signature and the expiry time. Nothing on the
server recorded that a session had ended.

Logout deleted the cookie in the browser that asked. A copy of the token, taken
from a shared machine, a proxy log, or an exported browser profile, kept working
for the rest of its 24 hours. The remediation plan (Phase 4.5) listed
revocation, and the authorization middleware said it was "tracked separately",
but it was never built.

It also mattered for auditing. Logout writes an `auth.logout` row
([ADR 0034](0034-audit-trail-scope.md)), and a token that survived logout could
replay that call and keep writing rows.

### What already exists

- `monorepo/apps/api/src/routers/auth.ts` issues the token (`issueSession`) and
  handles logout.
- `monorepo/apps/api/src/middleware/auth.ts` verifies the token on every
  request.
- The `users` table, created by migration `0001_core_schema.sql`.

---

## Options considered

### A. A session version per user (chosen)

A `session_version` number on each user, copied into the token as `sv`. The
middleware compares the two on each request, and logout increments the number.

It needs one integer column and one indexed read per request, and nothing has
to be cleaned up later. The cost is that logout ends all of a user's sessions,
not only the one that asked.

### B. A list of revoked tokens

Give each token an id. Logout stores the id until the token would have expired,
and every request checks the list.

This ends exactly one session, which is more precise. But it needs a table that
grows with every logout and a job to delete expired rows, and it still needs a
database read per request. For a dashboard where one person rarely holds many
sessions, the precision does not pay for the extra moving parts.

### C. A shorter token with renewal

Cut the token to about an hour and renew it while the user is active. There is
no database read at all.

It shrinks the window but does not close it: a copied cookie still works until
it expires, and logout is still not revocation. It also needs renewal logic in
the dashboard.

---

## Decision

### Schema

Migration `0009_user_session_version.sql` adds
`users.session_version INTEGER NOT NULL DEFAULT 0`. The Drizzle mirror in
`packages/db/schemas/user.ts` declares it as `sessionVersion`.

### Logic

- `issueSession` puts the user's current `session_version` into the token as
  the `sv` claim.
- `authMiddleware` verifies the signature, then reads the user's
  `session_version` by primary key. The request is refused with 401, and the
  cookie cleared, when the token has no `sv`, when `sv` does not match, or when
  the user no longer exists.
- If the database cannot be read, the request fails. Letting a session through
  without knowing whether it was revoked would defeat the check.
- Logout increments `session_version`, then clears the cookie and writes the
  audit row.
- Signing in does not change the number. Signing in on a second device must not
  sign the user out of the first.
- The number is internal and is removed from user objects in API responses,
  alongside the password hash.

### API surface

`POST /auth/logout` keeps its shape. Its contract now says it ends every session
and requires one. The contract previously marked it as public, which it had
not been since Phase 4.

### Migration

Tokens issued before this change carry no `sv`, so they are refused. Every user
signs in once more after the upgrade. No data is lost.

---

## Consequences

**Good.**

- A copied cookie stops working the moment its user logs out.
- A replayed logout can no longer keep writing audit rows, which strengthens the
  bound ADR 0034 relies on.
- The same read could later supply the user's current role. Today the role still
  comes from the token, so a role change waits for the next sign-in.

**Costs.**

- One extra database read on every authenticated request.
- Logging out on one device signs the user out everywhere.

**Accepted risks.**

- A token copied while the user stays signed in still works until it expires
  or the user logs out. Ending a single session without logout needs option B
  or an administrator action, which does not exist yet.
- When the database is down, authenticated requests fail. They already did,
  because every dashboard route reads the database.

---

## Related

- [ADR 0033](0033-role-model-and-access-control.md): the roles carried in the
  same token. Their check runs after this one.
- [ADR 0034](0034-audit-trail-scope.md): the audit rows that logout writes, and
  the rate limit that bounds them.
