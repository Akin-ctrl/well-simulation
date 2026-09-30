# Production Readiness

This checklist defines what "production-grade" should mean for this repository.

## Documentation

- Root README clearly states what the system is and is not.
- ADRs exist for all major architectural decisions.
- Local setup, deployment, and troubleshooting docs exist.
- API contracts are documented.
- Data model and migration strategy are documented.

## Reliability

- Services use health checks instead of fixed startup sleeps.
- Services retry transient database and Modbus failures with backoff.
- Docker Compose uses correct internal ports and paths.
- The API, dashboard, database, simulator, gateway, and ingestion services run together.
- Ingestion handles malformed Modbus reads without stopping the pipeline.

## Security

- Secrets are loaded from environment variables or secret stores.
- `.env.example` exists; real `.env` files are ignored.
- Auth never returns password hashes.
- Login issues secure HTTP-only cookies or bearer tokens.
- Protected routes validate sessions consistently.
- Public ports are intentional and documented.

## Secret Management

Secrets are read from environment variables. Nothing reads a credential from a
file, a config table, or a hardcoded default, so moving to a secret store means
changing how the environment is populated and nothing else.

What holds today:

- `.env` is git-ignored. `.env.example` carries placeholders only.
- Every service reads its secrets through one function: `required_env` in
  `data/src/telemetry_common.py`, and `apps/api/src/config.ts` for the API.
- `JWT_SECRET` is validated at boot and the service refuses to start without a
  usable one. A misconfigured deployment fails immediately rather than serving
  500s.
- No secret is written to a log. The API redacts bound query parameters before
  logging a database error, because Drizzle includes them in the message.

What a real deployment changes, and only this:

- Compose reads `.env`. A different orchestrator injects the same variable
  names from its own secret store: Kubernetes Secrets, AWS Secrets Manager, or
  Vault with an agent that populates the environment.
- The names are already the interface. `POSTGRES_PASSWORD` and `JWT_SECRET` do
  not care where their value came from.

What is still missing:

- No rotation. Changing `JWT_SECRET` invalidates every session at once, because
  tokens carry no key id. Supporting rotation means signing with a key id and
  accepting the previous key during a window.
- The demo account password is in the repository on purpose, and the bootstrap
  administrator password is generated and logged once. Both are local-stack
  conveniences and neither belongs in a real deployment.

## Data

- There is one canonical schema migration strategy.
- Time-series tables, views, and continuous aggregates are reproducible.
- The system distinguishes raw readings, model state, predictions, and scenario results.
- Database constraints protect asset and parameter integrity.

## Observability

- Services emit structured logs.
- Health endpoints exist for API and model services.
- Metrics are available for ingestion lag, Modbus errors, database writes, alarm count, and model update duration.
- Errors include enough context for debugging without leaking secrets.

## Testing

- Unit tests cover model behavior and alarm rules.
- Integration tests cover ingestion and API/database access.
- Contract tests cover dashboard/API responses.
- Scenario tests verify what-if simulation behavior.
- CI runs formatting, linting, type checks, and tests.

