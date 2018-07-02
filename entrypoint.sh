#!/bin/bash
set -e

if [ "$1" = 'sam-linker' ]; then
    exec /app/sam-linker
fi

exec "$@"
