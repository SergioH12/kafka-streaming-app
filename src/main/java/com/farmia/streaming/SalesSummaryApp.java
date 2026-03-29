package com.farmia.streaming;

import com.farmia.sales.SalesTransaction;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class SalesSummaryApp {

    private static final Logger logger = LoggerFactory.getLogger(SalesSummaryApp.class.getName());

    public static void main(String[] args) throws IOException {
        // Cargamos la configuración
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sales-summary-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        final String inputTopic = "sales_transactions";
        final String outputTopic = "sales-summary";

        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", "http://localhost:8081");
        //Creamos un Serde de tipo Avro ya que el productor produce <String,SalesTransaction>
        Serde<SalesTransaction> salesTransactionSerde = new SpecificAvroSerde<>();
        salesTransactionSerde.configure(serdeConfig, false);

        // Construcción del stream
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, com.farmia.sales.SalesTransaction> salesStream =
            builder.stream(inputTopic, Consumed.with(Serdes.String(), salesTransactionSerde));
            salesStream
                    .peek((key, value) -> logger.info("Processing transaction: {} - Category: {}, Qty: {}, Price: {}",
                            value.getTransactionId(), value.getCategory(), value.getQuantity(), value.getPrice()))
                    .groupBy((key, value) -> value.getCategory().toString(),
                            Grouped.with(Serdes.String(), salesTransactionSerde))
                    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
                    .aggregate(
                            () -> new double[]{0, 0}, // [total_quantity, total_revenue]
                            (category, transaction, aggregate) -> {
                                aggregate[0] += transaction.getQuantity();
                                aggregate[1] += transaction.getQuantity() * transaction.getPrice();
                                return aggregate;
                            },
                            Materialized.with(Serdes.String(), Serdes.serdeFrom(
                                    (topic, data) -> {
                                        if (data == null) return null;
                                        java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(16);
                                        buffer.putDouble(data[0]);
                                        buffer.putDouble(data[1]);
                                        return buffer.array();
                                    },
                                    (topic, bytes) -> {
                                        if (bytes == null) return null;
                                        java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(bytes);
                                        return new double[]{buffer.getDouble(), buffer.getDouble()};
                                    }
                            ))
                    )
                    .toStream()
                    .map((windowedKey, totals) -> {
                        String summary = createSummary(
                                windowedKey.key(),
                                (int) totals[0],
                                totals[1],
                                windowedKey.window().start(),
                                windowedKey.window().end()
                        );
                        return KeyValue.pair(windowedKey.key(), summary);
                    })
                    .peek((key, value) -> logger.info("Outgoing record - key: {} value: {}", key, value))
                    .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static String createSummary(String category, int totalQuantity,
                                        double totalRevenue, long windowStart, long windowEnd) {
        return String.format(
                "{\"category\":\"%s\",\"total_quantity\":%d,\"total_revenue\":%.2f,\"window_start\":%d,\"window_end\":%d}",
                category, totalQuantity, totalRevenue, windowStart, windowEnd
        );
    }
}
