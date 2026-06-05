# ADR 0016: Use REST APIs Documented With OpenAPI

- Status: Accepted
- Date: 2026-05-31

## Context

The project has a TypeScript API, a future Python twin-core service, and a React dashboard. The API contract should be clear, production-friendly, and language-neutral.

## Options Considered

### Option A: Ad hoc REST

- Pros: Fastest.
- Cons: Contracts drift and documentation becomes unreliable.

### Option B: REST with OpenAPI

- Pros: Clear, testable, language-neutral, and production-friendly.
- Cons: Requires maintaining schemas/specs.

### Option C: tRPC/RPC-style TypeScript contract

- Pros: Excellent TypeScript developer experience.
- Cons: Less suitable for Python service boundaries.

### Option D: GraphQL

- Pros: Flexible dashboard querying.
- Cons: Unnecessary complexity at this stage.

## Decision

Use **Option B: REST APIs documented with OpenAPI**.

## Rationale

REST plus OpenAPI works across TypeScript and Python, is easy to review, and supports future generated clients and contract testing.

## Consequences

- The TypeScript API is the public product API used by the dashboard.
- The Python twin-core API is internal.
- Browser clients should not call `twin-core` directly.
- API changes should update OpenAPI documentation.

