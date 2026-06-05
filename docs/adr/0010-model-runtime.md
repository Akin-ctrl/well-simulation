# ADR 0010: Run The Twin Core As A Separate Python Service

- Status: Accepted
- Date: 2026-05-31

## Context

The twin core will own model state, process updates, forecasts, and what-if simulations. It should have a clear boundary from the dashboard/API and from the Modbus protocol adapter.

## Options Considered

### Option A: Keep model logic inside the existing simulator script

- Pros: Fastest path from current code.
- Cons: Risks creating a god-script that mixes model state, telemetry output, scenarios, and service concerns.

### Option B: Create a separate Python twin-core service

- Pros: Clear boundary; strong fit for simulation/math; independently testable.
- Cons: Adds another service to Docker Compose.

### Option C: Put model logic inside the TypeScript API

- Pros: Fewer services; easy dashboard integration.
- Cons: Mixes web/API concerns with process modeling; less suitable for future numerical work.

### Option D: Use a specialized simulation framework immediately

- Pros: More formal simulation structure.
- Cons: Premature and may distract from the first credible twin version.

## Decision

Use **Option B: a separate Python twin-core service**.

## Rationale

Python is the best fit for the model layer now and leaves room for NumPy, SciPy, Pandas, or other modeling tools later. A separate service keeps the TypeScript API focused on product-facing concerns and keeps protocol adapters independent.

## Consequences

- Docker Compose should include a `twin-core` service.
- The current simulator should evolve toward or be replaced by this service.
- The twin core should be tested independently from API and Modbus code.

