# OpenAPI Contracts

This directory contains committed API specifications for the wellhead monitoring simulation and its planned lightweight digital-twin evolution.

## Specifications

- `public-api.yaml` — dashboard-facing TypeScript/Hono API implemented by `monorepo/apps/api`.
- `twin-core-internal-api.yaml` — planned internal Python twin-core API. This is a design contract, not an implemented service yet.

## Rules

- Specs must describe real or explicitly planned service boundaries.
- Future endpoints must be marked clearly with descriptions such as `planned`.
- API responses must not expose secrets, password hashes, raw stack traces, or internal database errors.
- Breaking changes should be reflected in an ADR when they affect architecture or portfolio narrative.

## Validation Target

Run the local contract hygiene check from the monorepo:

```sh
npx --yes pnpm@10.11.1 validate:openapi
```

This checks that each committed spec parses, declares OpenAPI 3.x metadata, has paths, uses unique operation IDs, declares success responses, and resolves internal `$ref` values.

The next production-readiness step is to run this in CI and optionally add a stricter OpenAPI linter.
