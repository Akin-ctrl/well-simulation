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

  cp .env.example .env                     then set POSTGRES_PASSWORD,
                                           TWIN_DB_PASSWORD and JWT_SECRET
  docker compose up --build                dashboard on port 8090

EOF
