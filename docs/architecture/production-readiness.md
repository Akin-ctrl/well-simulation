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

