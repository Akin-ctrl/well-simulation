# Security Policy

This project simulates an industrial wellhead monitoring system. It runs
locally with Docker Compose and is not deployed as a product. Reports are
still welcome, and they are handled privately.

## Reporting a vulnerability

**Do not open a public issue.**

Use GitHub's private vulnerability reporting:

**https://github.com/Akin-ctrl/well-simulation/security/advisories/new**

Only you and the maintainer can see a report made there.

Helpful things to include, as far as you have them:

- what an attacker gains, and what access they need to start
- the affected service: `api`, `dashboard`, `twin-core`, `modbus`,
  `ingestion` or `migrator`
- the commit you tested
- steps to reproduce, or a proof of concept

## What to expect

| | |
|---|---|
| Acknowledgement | within 7 days |
| Initial assessment | within 14 days, including whether it is in scope and how severe it is |
| Credit | offered by default, declined on request |

If you do not hear back within 7 days, please assume the report went astray
and send it again rather than waiting.

## Scope

**In scope:** the API and its authentication and access control, the
dashboard, the twin-core service, the Modbus gateway, the ingestion service,
the SQL migrations in `data/sql/migrations`, the Docker images and
`docker-compose.yml`, and the CI workflow in `.github/workflows/`.

**Out of scope**, because these are known and documented:

- Modbus TCP has no authentication or encryption. That is how the protocol
  works. Anyone who can reach port 5020 can talk to the gateway. Protection
  for Modbus has to come from the network around it.
- The demo account password is in the repository on purpose, and the
  bootstrap administrator password is logged once at startup. Both are
  conveniences for the local stack.
- `JWT_SECRET` cannot be rotated without signing every user out.
- Vulnerabilities in third-party dependencies that already have a published
  advisory. Dependabot tracks those. Do say if the way this project uses one
  makes it worse.
- Findings that need an already-compromised host.

The full list of known gaps is in
[docs/architecture/production-readiness.md](../docs/architecture/production-readiness.md).
Anything listed there as missing is a known gap, not a new finding.

## Disclosure

Coordinated. The maintainer will agree a disclosure date with you, and
publishes a GitHub security advisory once a fix is available. If a fix will
take longer than 90 days, the reason will be explained rather than left
silent.

## Supported versions

There are no releases yet. Fixes land on `main`, and only the latest `main`
is supported.
