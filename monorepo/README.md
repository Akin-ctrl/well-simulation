# Well Simulation Web Monorepo

This monorepo contains the TypeScript web/API layer for the Wellhead Monitoring Simulation.

The wider repository is heading toward a lightweight wellhead digital twin. This monorepo is not the twin core. It holds the operator dashboard, the API boundary, the shared contracts, the database access layer, and the UI components.

## What Lives Here

| Path                | Purpose                                                                                        |
| ------------------- | ---------------------------------------------------------------------------------------------- |
| `apps/api`          | Hono API used by the dashboard. Handles auth, dashboard reads, health, readiness, and metrics. |
| `apps/website`      | React Router/Vite dashboard served behind Nginx in Docker Compose.                             |
| `packages/db`       | Drizzle database schema, query helper, and migration tooling.                                  |
| `packages/dto`      | Shared request/response DTOs and validation schemas.                                           |
| `packages/ui`       | Shared React UI primitives used by the dashboard.                                              |
| `packages/utils`    | Shared utility functions and runtime configuration helpers.                                    |
| `packages/tsconfig` | Shared TypeScript configuration.                                                               |

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

The Python model in `../twin_core` calculates all 18 seeded signals. The gateway
and ingestion services under `../data` carry them through Modbus to the
historian. The public API also requests forecasts and scenarios from twin-core
over authenticated internal HTTP, then reads saved runs from PostgreSQL.

## API Contracts

Committed API contracts live in `../docs/openapi`:

- `../docs/openapi/public-api.yaml` documents the dashboard-facing API implemented by `apps/api`.
- `../docs/openapi/twin-core-internal-api.yaml` documents the implemented
  twin-core reads, forecasts, and scenarios, and labels future operations as planned.

These contracts are intentionally lightweight. They are designed to support portfolio review, implementation planning, and future CI validation without pretending that the project already has a complete industrial control API.

## Local Development

From `monorepo/`:

```sh
npx --yes pnpm@10.11.1 install
npx --yes pnpm@10.11.1 --filter @well-simulation/api dev
npx --yes pnpm@10.11.1 --filter @well-simulation/website dev
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
cd packages/ui && ../../node_modules/.bin/eslint . && ../../node_modules/.bin/tsc --noEmit
```

## Production-Grade Expectations

- Keep API responses free of secrets and password hashes.
- Keep runtime configuration explicit and validated.
- Prefer typed shared DTOs over ad-hoc payload shapes.
- Keep generated framework files out of lint targets.
- Keep dashboard data access behind documented API endpoints.
- Keep twin simulation logic out of the React dashboard.

## Known Gaps

- The dashboard has no model state, forecast, or what-if forms. The model and
  analysis APIs are implemented; their dashboard views remain deferred.
- The API contract is hand-maintained. CI validates its structure and references,
  but it does not compare every response field against running endpoints.
- Auth has login, logout, current-user, and administrator-only user creation.
  Logout revokes all current sessions. Role and account editing have no public
  route yet; current JWT role claims can outlive a database role change.
- Live model commands and parameter proposals are available through the API.
  The dashboard has no forms for them. Control writes recheck account authority
  in the database, including when twin-core applies queued work.
