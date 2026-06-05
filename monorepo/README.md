# Corsight Web Monorepo

This monorepo contains the TypeScript web/API layer for the Wellhead Monitoring Simulation.

The wider repository is evolving toward a lightweight wellhead digital twin, but this monorepo is not the twin core. Its role is to provide the operator-facing dashboard, API boundary, shared contracts, database access layer, and reusable UI components that make the telemetry simulation understandable and demoable.

## What Lives Here

| Path | Purpose |
| --- | --- |
| `apps/api` | Hono API used by the dashboard. Handles auth, dashboard reads, health, and readiness endpoints. |
| `apps/website` | React Router/Vite dashboard served behind Nginx in Docker Compose. |
| `packages/db` | Drizzle database schema, query helper, and migration tooling. |
| `packages/dto` | Shared request/response DTOs and validation schemas. |
| `packages/ui` | Shared React UI primitives used by the dashboard. |
| `packages/utils` | Shared utility functions and runtime configuration helpers. |
| `packages/tsconfig` | Shared TypeScript configuration. |

## Current Runtime Shape

```text
Browser
  |
  v
Nginx dashboard container
  |-- serves React assets
  |-- proxies /api/* to apps/api
  v
Hono API
  |
  v
PostgreSQL/TimescaleDB historian
```

Telemetry still originates from the Python simulator and Modbus ingestion services under `../data`. The future `twin-core` service will live outside this monorepo unless a later ADR changes that boundary.

## API Contracts

Committed API contracts live in `../docs/openapi`:

- `../docs/openapi/public-api.yaml` documents the dashboard-facing API implemented by `apps/api`.
- `../docs/openapi/twin-core-internal-api.yaml` documents the planned internal API for the future Python twin-core service.

These contracts are intentionally lightweight. They are designed to support portfolio review, implementation planning, and future CI validation without pretending that the project already has a complete industrial control API.

## Local Development

From `monorepo/`:

```sh
npx --yes pnpm@10.11.1 install
npx --yes pnpm@10.11.1 --filter @corsight/api dev
npx --yes pnpm@10.11.1 --filter @corsight/website dev
```

For the full stack, prefer the root Docker Compose file:

```sh
cd ..
docker compose up --build
```

## Validation

Targeted checks used during this standards pass:

```sh
./node_modules/.bin/tsc -p apps/api/tsconfig.json --noEmit
./node_modules/.bin/tsc -p apps/website/tsconfig.json --noEmit
cd apps/website && ../../node_modules/.bin/eslint .
cd packages/ui && ../../node_modules/.bin/biome lint . && ../../node_modules/.bin/tsc --noEmit
```

## Production-Grade Expectations

- Keep API responses free of secrets and password hashes.
- Keep runtime configuration explicit and validated.
- Prefer typed shared DTOs over ad-hoc payload shapes.
- Keep generated framework files out of lint targets.
- Keep dashboard data access behind documented API endpoints.
- Keep twin simulation logic out of the React dashboard.

## Known Gaps

- The dashboard still contains placeholder views that need live data integration.
- The API contract is hand-maintained; schema validation in CI is not wired yet.
- Auth has login/register and secure cookie issuance, but still needs logout and current-user endpoints.
- The future twin-core service is documented but not implemented yet.
