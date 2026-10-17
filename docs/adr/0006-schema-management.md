# ADR 0006: Keep SQL As The Canonical Schema Source

- Status: Accepted, amended
- Date: 2026-05-31
- Implementation: Implemented and adopted on the local database

## Context

The repository currently has SQL bootstrap files under `data/sql/` and Drizzle schema/migrations under `monorepo/packages/db/`. These are not fully aligned.

TimescaleDB-specific features such as hypertables, continuous aggregates, and refresh policies are easier to express directly in SQL.

## Options Considered

### Option A: Make Drizzle the only schema source

- Pros: TypeScript-first workflow; typed app access.
- Cons: TimescaleDB extension features and continuous aggregate policies are less natural.

### Option B: Make raw SQL migrations canonical and generate/use Drizzle for typed access

- Pros: Best control over PostgreSQL/TimescaleDB features; works well for database-first industrial systems.
- Cons: Requires discipline to keep TypeScript schema definitions aligned.

### Option C: Keep both independently

- Pros: No immediate migration work.
- Cons: Schema drift will continue and production confidence stays low.

## Decision

Use **raw SQL migrations as the canonical schema source** and keep Drizzle schema as the typed application access layer.

## Rationale

This project is database-heavy and depends on PostgreSQL/TimescaleDB capabilities. SQL should own the database contract; Drizzle should mirror it for type-safe queries.

## Consequences

- Existing SQL bootstrap files should become ordered migrations.
- Drizzle schema must be audited against SQL migrations.
- CI should verify migrations apply cleanly.
- Schema changes require both SQL migration updates and Drizzle type updates.

## Amendment 2026-10-13: Maintain one current schema file

### Context

The owner asked on 2026-10-03 to combine all schema definitions into one file
and questioned whether that file must be SQL. The follow-up requested more
explanation. The discussion remained unresolved while more files were added.
This preference must be considered before further schema work.

### Decision

The owner selected one SQL schema file on 2026-10-13. Maintain the complete
current database definition in data/sql/schema.sql. The owner authorized
implementation after choosing this format.
Fold old column additions and constraint corrections into the final definitions.
Keep demo seed records separate from database structure. Python continues to
own installation, verification, generated credentials, and configured grants.

Adopt an existing, fully current database only after checking both its recorded
history and actual structure against the consolidated definition. Preserve its
readings, application records, and old schema_migrations ledger. Do not replay
historical view drops or replace the existing Docker volume.

The installer provides fresh installation, checking, and safe adoption.
It does not infer arbitrary upgrades after later schema edits.
Changes to an existing database still require a reviewed conversion plan.

### Options considered

One SQL file provides a direct definition of the TimescaleDB objects already
used here. A Python or TypeScript source is possible, but still needs SQL for
database-specific functions and policies. A comparison tool may help with future
upgrades, but must first prove coverage for the project's TimescaleDB objects.

### Consequences

**Good.** One file contains the complete current schema. Startup compares the
actual database with a generated reference and rejects structural drift.

**Costs.** The reference must be regenerated from an empty database after a
schema change. Existing data still needs a tested conversion plan. Version
changes require new reference verification; Compose and CI use a pinned image.

**Accepted risks.** The installer cannot infer arbitrary data conversions.
An interrupted installation requires investigation, because some TimescaleDB
statements cannot share one transaction. It stops instead of replaying them.

### Reversal

The numbered schema files are replaced by the current definition. Git preserves
their source history; adopted databases retain their original ledger entries.
Do not reintroduce a numbered corrective-file chain as the default authoring
workflow. A generated overview beside the old chain would not satisfy this decision.

The [consolidation design](../design/consolidated-schema.md) records the
layout, installer behavior, failure cases, and verification gates. The original
decision above remains as history; this amendment governs new schema work.

## Amendment 2026-10-16: Accept verified engine changes explicitly

The generated reference includes engine versions. A reviewed engine upgrade
therefore needs an explicit acceptance step even when schema.sql stays unchanged.
The accept-engine command requires the saved previous reference to match the
installed identity, preserves the SQL and legacy-history checksums, and verifies
the current engine and actual database structure against the new reference.
It updates the recorded identity and writes an audit event in one transaction.

Normal startup still refuses a mismatched identity. Acceptance does not apply
schema statements or replace backup and application testing. Comparison allows
the verified PostgreSQL difference between casting a list of text values as a
whole and casting each value individually. It still detects changed values,
types, expressions, and unrelated structure.

This adds an explicit operational step and tests for engine compatibility. It
keeps one SQL source and avoids adding a new numbered-file workflow. See the
[engine upgrade design](../design/database-engine-upgrade.md) for its checks.


## Amendment 2026-10-17: Verify additions to the current schema

### Context

Scheduled forecasts and alarm lifecycle records need new ordinary tables. The
single canonical SQL file remains the owner's chosen workflow. Existing data must
survive additions without replaying the full bootstrap or creating a migration chain.

### Decision

Add an explicit `extend` installer mode. Supply the previous committed schema and
contract as temporary references. Require an exact source prefix, unchanged engine
and legacy identity, unchanged existing catalog definitions, and a permitted suffix
containing only new ordinary tables and their indexes. Apply that suffix, verify
the complete generated target catalog and record the new identity and audit event
in one transaction. Provision service grants afterward. Repeating a completed
extension checks its target without replaying DDL.

### Consequences

Fresh installations still use the complete `data/sql/schema.sql`. Existing
installations take a private backup and verify a restored copy before extending.
This mode refuses changes to existing tables or functions; those changes need a
separate reviewed design. Previous files remain temporary release references,
not maintained alternate schemas. See the [extension design](../design/schema-extension.md).

## Restore comparison update, 2026-10-17

A full backup rehearsal exposed a PostgreSQL parser difference in four scheduled
forecast duration checks. Restoring the dump flattens the nested AND produced
by BETWEEN, while retaining the same bounds and divisibility test. The schema
comparison recognizes only that exact verified expression shape. It still checks
constraint names, validation flags and every operand, and rejects other changes.
The canonical SQL, its checksum and the exact previous-source requirement remain
in force. See the [extension follow-up](../design/schema-extension.md#restore-comparison-follow-up-2026-10-17).
