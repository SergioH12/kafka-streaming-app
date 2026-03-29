
# Ingestion Engine

Motor de ingestión y procesamiento en tiempo real basado en **Apache Kafka Streams** y **Kafka Connect**.

Este repositorio está preparado para ejecutarse en cualquier entorno mediante Docker e incluye:
- Aplicaciones de procesamiento en Java
- Esquemas Avro
- Configuración de conectores
- Scripts de automatización

---

## Características

- Procesamiento en tiempo real con Kafka Streams
- Integración con sistemas externos (MySQL, MongoDB)
- Uso de Avro + Schema Registry
- Entorno reproducible con Docker Compose
- Casos de uso incluidos: IoT y analítica de ventas

---

## Requisitos

- Java 23  
- Maven 3.8+  
- Docker & Docker Compose  
- Git  

---

## Estructura del proyecto

```

.
├── pom.xml
├── src/
│   └── com.farmia.streaming/
│       ├── SensorAlerterApp.java
│       └── SalesSummaryApp.java
├── datagen/
│   ├── sensor-telemetry.avsc
│   └── transactions.avsc
├── connectors/
├── sql/
│   └── transactions.sql
├── docker/
│   └── docker-compose.yml
├── setup.sh
├── start_connectors.sh
└── shutdown.sh

````

---

## Quickstart

### 1. Clonar el repositorio

```bash
git clone <repo-url>
cd <repo-directory>
````

### 2. Construir el proyecto

```bash
mvn -U clean package
```

### 3. Levantar el entorno completo

```bash
./setup.sh
```

O manualmente:

```bash
cd docker
TAG=7.8.0 CLUSTER_ID=local docker compose up -d
```

---

## Aplicaciones

### SensorAlerterApp

Procesa telemetría de sensores IoT y genera alertas en tiempo real.

* **Clase:** `com.farmia.streaming.SensorAlerterApp`
* **Input topic:** `sensor-telemetry` (Avro)
* **Output topic:** `sensor-alerts` (JSON)

**Reglas:**

* `HIGH_TEMPERATURE` si `temperature > 35.0`
* `LOW_HUMIDITY` si `humidity < 20.0`

---

### SalesSummaryApp

Agrega transacciones de ventas por categoría en ventanas de tiempo.

* **Clase:** `com.farmia.streaming.SalesSummaryApp`
* **Input topic:** `sales_transactions` (Avro)
* **Output topic:** `sales-summary` (JSON)

**Características:**

* Ventanas tumbling de 1 minuto
* Métricas:

  * Cantidad total
  * Ingresos totales
* Acumulador: `double[2]` → `[total_quantity, total_revenue]`

---

## Topics

| Topic                | Descripción                   |
| -------------------- | ----------------------------- |
| `sensor-telemetry`   | Telemetría de sensores (Avro) |
| `sensor-alerts`      | Alertas generadas             |
| `sales_transactions` | Transacciones desde MySQL     |
| `sales-summary`      | Resumen agregado              |

---

## Conectores (Kafka Connect)

Ubicados en `connectors/`:

* Datagen source → `sensor-telemetry`
* Datagen source → `_transactions`
* JDBC Source (MySQL) → `sales_transactions`
* JDBC Sink → MySQL
* MongoDB Sink → `sensor-alerts`

---

## Ejecución

### Ejecutar aplicaciones localmente

```bash
mvn exec:java -Dexec.mainClass="com.farmia.streaming.SensorAlerterApp"

mvn exec:java -Dexec.mainClass="com.farmia.streaming.SalesSummaryApp"
```

---

## Prueba end-to-end

1. Levantar entorno:

```bash
./setup.sh
```

2. Registrar conectores:

```bash
./start_connectors.sh
```

3. Ejecutar aplicaciones

4. Consumir resultados:

```bash
kafka-console-consumer --topic sensor-alerts --bootstrap-server localhost:9092
```

---

## Detener el entorno

```bash
./shutdown.sh
```

---

## Configuración

* **Kafka Brokers:**
  `localhost:9092,9093,9094`

* **Schema Registry:**
  `http://localhost:8081`

---

## Notas

* Los esquemas Avro se encuentran en `datagen/`
* Los scripts automatizan la configuración del entorno
* El proyecto está diseñado para ser portable y reproducible

