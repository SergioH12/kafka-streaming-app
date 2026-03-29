#!/usr/bin/env bash

set -euo pipefail

COMPOSE_FILE="docker/docker-compose.yml"

echo "Iniciando entorno (usando $COMPOSE_FILE)"
docker compose -f "$COMPOSE_FILE" up -d
sleep 50

if [ -f sql/transactions.sql ]; then
	echo "Creando la tabla transactions"
	docker cp sql/transactions.sql mysql:/transactions.sql
	docker exec mysql bash -c "mysql --user=root --password=password --database=db < /transactions.sql"
else
	echo "Aviso: sql/transactions.sql no encontrado — omitiendo creación de tabla"
fi

echo "Instalando conectores (si procede)"
docker compose -f "$COMPOSE_FILE" exec connect confluent-hub install --no-prompt confluentinc/kafka-connect-datagen:latest || true
docker compose -f "$COMPOSE_FILE" exec connect confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:latest || true
docker compose -f "$COMPOSE_FILE" exec connect confluent-hub install --no-prompt mongodb/kafka-connect-mongodb:latest || true

if [ -f 1.environment/mysql/mysql-connector-java-5.1.45.jar ]; then
	echo "Copiando driver MySQL al contenedor connect"
	docker cp 1.environment/mysql/mysql-connector-java-5.1.45.jar connect:/usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/mysql-connector-java-5.1.45.jar
else
	echo "Aviso: driver MySQL no encontrado en 1.environment/mysql/... — omitiendo"
fi

echo "Copiando schemas AVRO (si existen)"
if [ -f datagen/sensor-telemetry.avsc ]; then
	docker cp datagen/sensor-telemetry.avsc connect:/home/appuser/
fi
if [ -f datagen/transactions.avsc ]; then
	docker cp datagen/transactions.avsc connect:/home/appuser/
fi

echo "Reiniciando contenedor connect..."
docker compose -f "$COMPOSE_FILE" restart connect || true
echo "Esperando reinicio contenedor connect"
sleep 30

echo "OK"
