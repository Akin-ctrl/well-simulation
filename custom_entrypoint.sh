# #!/bin/bash
# set -e

# # This script will be executed by the Docker entrypoint.
# # It ensures our SQL files are run in the correct order.

# echo "Running custom entrypoint script..."

# # Execute init.sql first
# echo "Executing init.sql..."
# psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/init.sql

# # Execute create_views.sql second
# echo "Executing create_views.sql..."
# psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/create_views.sql

# echo "Custom entrypoint script finished."


#!/bin/bash
set -e

echo "Running custom entrypoint script..."

# Location of the Postgres data directory
DATA_DIR="/var/lib/postgresql/data"
MARKER_FILE="$DATA_DIR/.db_initialized"

# Only run if database hasn't been initialized before
if [ -f "$MARKER_FILE" ]; then
    echo "Database already initialized. Skipping SQL execution."
    exit 0
fi

# Wait for Postgres to be ready before executing SQL
until pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" > /dev/null 2>&1; do
    echo "Waiting for PostgreSQL to be ready..."
    sleep 2
done

echo "Executing init.sql..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/init.sql

echo "Executing create_views.sql..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/create_views.sql

# Mark as initialized
touch "$MARKER_FILE"

echo "Custom entrypoint script finished."
