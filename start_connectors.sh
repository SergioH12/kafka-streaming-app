#!/usr/bin/env bash

set -euo pipefail

CONNECT_URL="http://localhost:8083"

echo "Registrando conectores en $CONNECT_URL"

register() {
	local file="$1"
	if [ -f "$file" ]; then
		echo "POST $file"
		curl -s -X POST -H "Content-Type: application/json" --data-binary "@$file" "$CONNECT_URL/connectors" || echo "Error registrando $file"
	else
		echo "Aviso: fichero $file no encontrado, omitiendo"
	fi
}

register "connectors/source-datagen-_transactions.json"
register "connectors/sink-mysql-_transactions.json"
register "connectors/source-datagen-sensor-telemetry.json"
register "connectors/source-mysql-transactions.json"
register "connectors/sink-mongodb-sensor_alerts.json"

echo "OK"
