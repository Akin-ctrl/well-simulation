# OpenAPI Contracts

This directory describes the implemented public API and internal model service.

## Specifications

- `public-api.yaml`: dashboard-facing TypeScript/Hono API implemented by `monorepo/apps/api`.
  Includes model status, saved analyses and their original inputs, historical
  scenario sources, scheduled forecasts, live synthetic controls and parameter
  publication. It also documents alarm acknowledgement, timed shelves and
  action receipt lookup, plus the unauthenticated service metrics endpoint.
- `twin-core-internal-api.yaml`: implemented internal reads and operational
  endpoints, protected model status, forecasts, scenarios, installed controls, command wake-up hints,
  and parameter validation for the Python twin-core service. Live targets are
  stored through the public command lifecycle; internal HTTP never applies
  a control payload directly.

## Rules

- Specs must describe real or explicitly planned service boundaries.
- Future endpoints must be marked clearly with descriptions such as `planned`.
- API responses must not expose secrets, password hashes, raw stack traces, or internal database errors.
- Breaking changes should be reflected in an ADR when they affect architecture or portfolio narrative.

## Validation Target

Run the local contract hygiene check from the monorepo:

```sh
pnpm validate:openapi
```

Install the Python dependencies in `requirements-dev.txt` first. The command
checks both specifications for OpenAPI metadata, paths, unique operation IDs,
success responses and valid internal references.

CI runs this structural check. Endpoint tests verify behavior separately; the
structural check alone cannot prove a running response matches every schema.
