# ADR 0028: Use Per-Service OpenAPI Specs Committed To Documentation

- Status: Accepted
- Date: 2026-05-31

## Context

The project has already selected REST APIs documented with OpenAPI. The next decision is how those OpenAPI specifications are created, reviewed, and kept aligned with implementation across TypeScript and Python services.

## Options Considered

### Option A: Handwritten OpenAPI YAML

- Pros: Explicit, reviewable, and language-neutral.
- Cons: Easy to drift from implementation.

### Option B: Generate OpenAPI from TypeScript/Hono API code only

- Pros: Keeps the public API spec close to implementation.
- Cons: Does not solve the Python twin-core API workflow.

### Option C: Generate OpenAPI from shared cross-language schemas

- Pros: Strong contract-first approach.
- Cons: More tooling complexity than the project needs initially.

### Option D: Code-first per service with committed OpenAPI artifacts

- Pros: Pragmatic, inspectable, and works across TypeScript and Python.
- Cons: Requires CI discipline to prevent stale specs.

## Decision

Use **Option D: code-first per service with generated or maintained OpenAPI specs committed under `docs/openapi`**.

## Rationale

This keeps contracts explicit and reviewable without forcing a heavy cross-language schema system too early. The public API and internal twin-core API have different audiences and should have separate specifications.

## Spec Locations

- `docs/openapi/public-api.yaml`
- `docs/openapi/twin-core-internal-api.yaml`

## Client Strategy

- Start with lightweight hand-written clients.
- Later generate TypeScript clients from the public API spec if the API stabilizes.
- Keep internal service clients explicit until twin-core boundaries stabilize.

## Consequences

- OpenAPI specs should be reviewed with API changes.
- CI should eventually validate that specs are syntactically valid.
- Contract tests should be added once endpoints stabilize.
- Public and internal API contracts remain separate.

