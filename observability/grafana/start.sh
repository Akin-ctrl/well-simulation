#!/bin/sh
set -eu
if [ "${#GF_SECURITY_ADMIN_PASSWORD}" -lt 32 ]; then
    printf '%s\n' 'Set GRAFANA_ADMIN_PASSWORD to at least 32 characters before starting the observability profile.' >&2
    exit 1
fi
exec /run.sh
