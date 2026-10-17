# Well simulation web and API

This workspace contains the dashboard, public API and shared TypeScript packages.
Users can inspect wells, save analyses and review operating actions. The Python
service in `../twin_core` owns the process model and calculations.

## What lives here

| Path                     | Purpose                                                                       |
| ------------------------ | ----------------------------------------------------------------------------- |
| `apps/api`               | Authentication, historian reads, model requests and audited operating actions |
| `apps/website`           | React Router dashboard, served through nginx in Compose                       |
| `packages/db`            | Drizzle table definitions and database queries, mirroring the canonical SQL   |
| `packages/dto`           | Shared request validation and response contracts                              |
| `packages/ui`            | Shared React components                                                       |
| `packages/utils`         | Shared utilities                                                              |
| `packages/eslint-config` | Shared lint rules                                                             |
| `packages/tsconfig`      | Shared TypeScript configuration                                               |

The database structure comes from [one SQL file](../data/sql/schema.sql).
Drizzle does not install or convert the schema. Follow the
[database update guide](../docs/operations/database-updates.md) for existing data.

## Current workflows

The dashboard includes fleet monitoring, trends, model status, forecasts,
scenarios from current or historical state, and saved-run history. Administrators
can configure scheduled forecasts and publish model parameters. OPERATIONS users
can review live synthetic control commands, acknowledge alarms and manage shelves.
These roles have separate authority. The seeded USER account can inspect data
and save analyses, but cannot submit operating actions.

The browser calls `/api` on its own origin. Nginx forwards those requests to Hono.
The API reads PostgreSQL and calls twin-core over authenticated internal HTTP.
The [architecture overview](../docs/architecture/overview.md) explains the data
paths and the separation between model experiments and running controls.

## Run the complete application

Follow the [root quick start](../README.md#quick-start), including the required
secret settings and any existing-database upgrade. From the repository root:

```sh
docker compose up -d --build --wait
```

Open <http://localhost:8090>. The root README lists the demo login.

## Develop the API and dashboard on the host

Use Node 22, recorded in `.nvmrc`, and pnpm 10.11.1, recorded in `package.json`.
Activate Node 22 with your version manager, then install the pinned pnpm version
if it is not already on PATH. Run from `monorepo/`:

```sh
npm install --global pnpm@10.11.1
pnpm install --frozen-lockfile
```

Keep the database, model and telemetry services in Compose. The host API needs
access to twin-core, which normally has no host port. From the repository root,
create a temporary development override and start the stack with it:

```sh
cat > /tmp/well-simulation-dev.compose.yml <<'YAML'
services:
  twin-core:
    ports:
      - "127.0.0.1:18000:8000"
YAML
docker compose -f docker-compose.yml -f /tmp/well-simulation-dev.compose.yml up -d --build --wait
```

In the ignored root `.env`, set `DATABASE_URL` to the restricted API login on
`127.0.0.1:5434`, using the same database name and `API_DB_PASSWORD` as Compose.
The example URL contains a placeholder password; changing `API_DB_PASSWORD`
does not update that URL. Keep `JWT_SECRET` and `TWIN_INTERNAL_TOKEN` equal to
the values used by the stack.

Start the host API from `monorepo/`:

```sh
pnpm exec dotenv -e ../.env -- env TWIN_CORE_URL=http://127.0.0.1:18000 ALLOWED_ORIGINS=http://127.0.0.1:5173 COOKIE_SECURE=false pnpm --filter @well-simulation/api dev
```

In a second terminal, also from `monorepo/`, start the dashboard:

```sh
SERVER_BASE_URL=http://127.0.0.1:3000 pnpm --filter @well-simulation/website dev --host 127.0.0.1 --port 5173 --strictPort
```

Open <http://127.0.0.1:5173>. Vite forwards `/api` to the host API on port 3000.
The explicit allowed origin lets that API accept browser form submissions.
These commands bypass Turbo, so it cannot filter out the model service settings.
The host API and Compose API share the same database; operating actions affect
the same synthetic wells.

When finished, stop both development processes. Run the normal Compose command
without the temporary override to remove the extra twin-core host port.

## Validation

Run these commands from `monorepo/`:

```sh
pnpm check-types
pnpm lint
pnpm format:check
pnpm test:dto
pnpm test:website
pnpm validate:openapi
```

OpenAPI validation also needs the Python dependencies in
`../requirements-dev.txt`. The website type task generates React Router types
before checking them; running `tsc` alone skips that step.

API integration tests need Python on PATH and a disposable TimescaleDB test
server. Supply `TEST_POSTGRES_HOST`, `TEST_POSTGRES_PORT`, `POSTGRES_USER` and
`POSTGRES_PASSWORD` for fixture administration, plus a separate `API_DB_USER`
and `API_DB_PASSWORD`. Setup drops and recreates `well_simulation_api_test`, or
the database named by `TEST_DATABASE_NAME`. Use a dedicated test server and
never point that name at data you need to keep. The tests exercise the API
through the restricted application account. Match the pinned server
image and setup in [CI](../.github/workflows/ci.yml), then run:

```sh
pnpm --filter @well-simulation/api test
```

With a healthy Compose application on port 8090, run browser checks:

```sh
pnpm --filter @well-simulation/website exec playwright install --with-deps chromium
pnpm --filter @well-simulation/website test:browser
```

Use `DASHBOARD_TEST_URL` for a separate test installation. The browser suite saves
synthetic analyses; it is not a read-only probe. See the
[verification records](../docs/README.md#interface-design-and-verification)
for real and mocked workflow coverage and accessibility limits.

## Contracts and remaining limits

The [public and internal API specifications](../docs/openapi/README.md) describe
the implemented routes. CI checks their structure and references. Endpoint tests
check behavior separately; neither establishes exhaustive response-schema coverage.

Account creation, login and logout exist. Public account editing, role changes,
alarm-rule editing and general asset administration do not. Ordinary read guards
use the session's role claim. Sensitive operating paths recheck current account
authority; scheduled jobs also check their owner's authority before saving.
Logout revokes every existing session for that account.

Keep model equations in Python, validation in shared contracts and data access
behind the API. See [production readiness](../docs/architecture/production-readiness.md)
for the current local evidence and open delivery items.
