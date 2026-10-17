#!/usr/bin/env bash
# Brings a fresh container to the point where the CI checks pass locally.
# It does not start the stack. That is one command away and downloads several
# images, so it stays your choice.
set -euo pipefail

echo "==> Python tools and the migrator's dependencies"
pip install --quiet -r requirements-dev.txt

echo "==> Workspace dependencies"
(cd monorepo && pnpm install --frozen-lockfile)

cat <<'EOF'

Ready.

  ruff check . && mypy && pytest           Python checks, as CI runs them
  (cd monorepo && pnpm turbo run check-types lint)
  python3 scripts/check_prose.py

  cp .env.example .env                     for a new local environment
                                           set separate POSTGRES_PASSWORD,
                                           TWIN_DB_PASSWORD, API_DB_PASSWORD,
                                           JWT_SECRET and TWIN_INTERNAL_TOKEN
  docker compose up --build                dashboard on port 8090

  README.md                               existing-database setup and demo login
  monorepo/README.md                       host development and full web checks

EOF
