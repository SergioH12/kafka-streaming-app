package com.farmia.streaming;

import com.farmia.iot.SensorTelemetry;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;


public class SensorAlerterApp {
    private static final Logger logger = LoggerFactory.getLogger(SensorAlerterApp.class.getName());
    private static final double TEMPERATURE_THRESHOLD = 35.0;
    private static final double HUMIDITY_THRESHOLD = 20.0;

    public static void main(String[] args) throws IOException {

        // Configuración de Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sensor-alerter-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");

        final String inputTopic = "sensor-telemetry";
        final String outputTopic = "sensor-alerts";

        // Creamos un Serde de tipo Avro ya que el productor produce SensorTelemetry
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", "http://localhost:8081");

        Serde<SensorTelemetry> sensorTelemetrySerde = new SpecificAvroSerde<>();
        sensorTelemetrySerde.configure(serdeConfig, false);

        // Creamos el KStream mediante el builder
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, SensorTelemetry> telemetryStream =
                builder.stream(inputTopic, Consumed.with(Serdes.String(), sensorTelemetrySerde));

        // Procesamiento: detección de anomalías y generación de alertas
        telemetryStream
                .peek((key, value) -> logger.info("Processing sensor: {} - Temp: {}°C, Humidity: {}%",
                        value.getSensorId(), value.getTemperature(), value.getHumidity()))
                .flatMap((key, value) -> {
                    var alerts = new java.util.ArrayList<org.apache.kafka.streams.KeyValue<String, String>>();

                    // Detectar temperatura alta
                    if (value.getTemperature() > TEMPERATURE_THRESHOLD) {
                        String alert = createAlert(
                                value.getSensorId().toString(),
                                "HIGH_TEMPERATURE",
                                value.getTimestamp().toEpochMilli(),
                                String.format("Temperature exceeded %.0fC", TEMPERATURE_THRESHOLD)
                        );
                        alerts.add(new org.apache.kafka.streams.KeyValue<>(key, alert));
                        logger.warn("HIGH_TEMPERATURE alert for sensor: {}", value.getSensorId());
                    }

                    // Detectar humedad baja
                    if (value.getHumidity() < HUMIDITY_THRESHOLD) {
                        String alert = createAlert(
                                value.getSensorId().toString(),
                                "LOW_HUMIDITY",
                                value.getTimestamp().toEpochMilli(),
                                String.format("Humidity below %.0f%%", HUMIDITY_THRESHOLD)
                        );
                        alerts.add(new org.apache.kafka.streams.KeyValue<>(key, alert));
                        logger.warn("LOW_HUMIDITY alert for sensor: {}", value.getSensorId());
                    }

                    return alerts;
                })
                .peek((key, alert) -> logger.info("Publishing alert: {}", alert))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        // Iniciar Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
    private static String createAlert(String sensorId, String alertType,long timestamp, String details) {
        return String.format(
                "{\"sensor_id\":\"%s\",\"alert_type\":\"%s\",\"timestamp\":%d,\"details\":\"%s\"}",
                sensorId, alertType, timestamp, details
        );
    }
}
