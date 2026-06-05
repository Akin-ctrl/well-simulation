# ADR 0006: Use SQL Migrations As The Canonical Schema Source

- Status: Accepted
- Date: 2026-05-31

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

