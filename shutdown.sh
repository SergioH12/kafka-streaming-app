#!/usr/bin/env bash

set -euo pipefail

COMPOSE_FILE="docker/docker-compose.yml"

echo "Deteniendo entorno (usando $COMPOSE_FILE)"
docker compose -f "$COMPOSE_FILE" down --remove-orphans
echo "OK"
