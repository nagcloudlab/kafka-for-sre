package com.example.producer;

import com.example.Transaction;
import com.example.JsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.math.BigDecimal;
import java.util.Properties;

/**
 * DEMO 1: Fire-and-Forget Producer
 * 
 * Delivery Semantic: At-Most-Once
 * - Message may be lost
 * - No delivery confirmation
 * - Highest throughput, lowest latency
 * 
 * Banking Scenario:
 * - Promotional notifications
 * - Non-critical analytics events
 * - Activity logging (where loss is acceptable)
 * 
 * KEY CONFIGS:
 * +-----------+-------+------------------------------------------+
 * | Config  | Value | Description |
 * +-----------+-------+------------------------------------------+
 * | acks    | 0     | Don't wait for any acknowledgment |
 * | retries | 0     | No retry on failure |
 * +-----------+-------+------------------------------------------+
 */
public class Demo01_FireAndForget {

    public static void main(String[] args) {

        Properties props = new Properties();

        // Bootstrap servers
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");

        // Serializers
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

        // Fire-and-Forget configs
        props.put(ProducerConfig.ACKS_CONFIG, "0"); // No acknowledgment
        props.put(ProducerConfig.RETRIES_CONFIG, 0); // No retries

        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(props);

        String topic = "fund-transfer-events";
        int messageCount = 10000;

        System.out.println("=".repeat(60));
        System.out.println("DEMO 1: Fire-and-Forget Producer");
        System.out.println("=".repeat(60));
        System.out.println("Config: acks=0, retries=0");
        System.out.println("Sending " + messageCount + " messages...\n");

        long startTime = System.currentTimeMillis();

        for (int i = 1; i <= messageCount; i++) {
            Transaction txn = Transaction.builder()
                    .fromAccount("ACC" + (i % 100))
                    .toAccount("ACC" + ((i + 1) % 100))
                    .amount(new BigDecimal("100.00"))
                    .type("NOTIFICATION")
                    .build();

            ProducerRecord<String, Transaction> record = new ProducerRecord<>(topic, txn.getTransactionId(), txn);

            // Fire and forget - no Future.get(), no callback
            producer.send(record);

            if (i % 2000 == 0) {
                System.out.println("Sent: " + i + " messages");
            }
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        producer.close();

        System.out.println("\n" + "=".repeat(60));
        System.out.println("BENCHMARK RESULTS");
        System.out.println("=".repeat(60));
        System.out.println("Total Messages  : " + messageCount);
        System.out.println("Total Time      : " + duration + " ms");
        System.out.println("Throughput      : " + (messageCount * 1000 / duration) + " msg/ms");
        System.out.println("Avg Latency     : " + (duration / (double) messageCount) + " ms/msg");
        System.out.println("=".repeat(60));
        System.out.println("\nWARNING: Some messages may be LOST - no confirmation!");
    }
}