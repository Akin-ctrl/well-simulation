#!/bin/sh
# Load dependencies for the old server without affecting the new server.
set -eu
export LD_LIBRARY_PATH=/opt/pg14-deps:/opt/pg14/lib
exec "/opt/pg14/bin/${0##*/}" "$@"
