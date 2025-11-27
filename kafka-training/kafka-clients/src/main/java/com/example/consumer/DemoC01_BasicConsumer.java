package com.example.consumer;

import com.example.Transaction;
import com.example.JsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * DEMO C01: Basic Consumer
 *
 * CONSUMER BASICS:
 * - Subscribe to topic(s)
 * - Poll for records in a loop
 * - Process records
 * - Offsets auto-committed by default
 *
 * POLL LOOP:
 * ┌─────────────────────────────────────────────────────────────────┐
 * │                                                                 │
 * │   while (running) {                                             │
 * │       records = consumer.poll(Duration.ofMillis(1000));         │
 * │       for (record : records) {                                  │
 * │           process(record);                                      │
 * │       }                                                         │
 * │   }                                                             │
 * │                                                                 │
 * └─────────────────────────────────────────────────────────────────┘
 *
 * KEY CONFIGS:
 * +---------------------------+-------------+--------------------------------+
 * | Config                    | Default     | Description                    |
 * +---------------------------+-------------+--------------------------------+
 * | bootstrap.servers         | (required)  | Kafka broker list              |
 * | group.id                  | (required)  | Consumer group identifier      |
 * | key.deserializer          | (required)  | Key deserializer class         |
 * | value.deserializer        | (required)  | Value deserializer class       |
 * | auto.offset.reset         | latest      | Where to start if no offset    |
 * | enable.auto.commit        | true        | Auto-commit offsets            |
 * +---------------------------+-------------+--------------------------------+
 *
 * Banking Scenario: Simple transaction event consumer
 */
public class DemoC01_BasicConsumer {

    public static void main(String[] args) {

        System.out.println("=".repeat(70));
        System.out.println("DEMO C01: Basic Consumer");
        System.out.println("=".repeat(70));
        System.out.println("Simple poll loop with auto-commit\n");

        Properties props = new Properties();

        // Required configs
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "basic-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());

        // Deserializer config for Transaction class
        props.put("json.deserializer.type", Transaction.class.getName());

        // Consumer behavior
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  // Start from beginning
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");     // Auto-commit offsets
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000"); // Commit every 5 sec

        KafkaConsumer<String, Transaction> consumer = new KafkaConsumer<>(props);

        String topic = "fund-transfer-events";

        // Subscribe to topic
        consumer.subscribe(Collections.singletonList(topic));
        System.out.printf("Subscribed to topic: %s%n", topic);
        System.out.printf("Consumer Group: %s%n", props.get(ConsumerConfig.GROUP_ID_CONFIG));
        System.out.println("\nWaiting for messages (Ctrl+C to stop)...\n");

        System.out.println("-".repeat(70));
        System.out.println("CONFIG SUMMARY:");
        System.out.println("-".repeat(70));
        System.out.println("  auto.offset.reset     = earliest (start from beginning)");
        System.out.println("  enable.auto.commit    = true");
        System.out.println("  auto.commit.interval  = 5000 ms");
        System.out.println("-".repeat(70));
        System.out.println("");

        int totalMessages = 0;
        int pollCount = 0;
        int maxPolls = 10;  // Limit for demo

        try {
            while (pollCount < maxPolls) {
                // Poll for records (blocking for up to 1 second)
                ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(1000));

                pollCount++;

                if (records.isEmpty()) {
                    System.out.printf("Poll #%d: No records (waiting...)%n", pollCount);
                    continue;
                }

                System.out.printf("Poll #%d: Received %d records%n", pollCount, records.count());

                for (ConsumerRecord<String, Transaction> record : records) {
                    totalMessages++;

                    // Process record
                    if (totalMessages <= 5) {
                        System.out.println("  ┌─────────────────────────────────────────────────────────");
                        System.out.printf("  │ Message #%d%n", totalMessages);
                        System.out.printf("  │ Topic: %s, Partition: %d, Offset: %d%n",
                                record.topic(), record.partition(), record.offset());
                        System.out.printf("  │ Key: %s%n", record.key());
                        System.out.printf("  │ Timestamp: %d%n", record.timestamp());

                        Transaction txn = record.value();
                        if (txn != null) {
                            System.out.printf("  │ Transaction: %s → %s, ₹%s%n",
                                    txn.getFromAccount(), txn.getToAccount(), txn.getAmount());
                        }
                        System.out.println("  └─────────────────────────────────────────────────────────");
                    }
                }

                // Auto-commit happens automatically in background
                System.out.printf("  (Auto-commit will happen every 5 seconds)%n%n");
            }

        } finally {
            System.out.println("\n" + "=".repeat(70));
            System.out.println("SUMMARY");
            System.out.println("=".repeat(70));
            System.out.printf("Total polls: %d%n", pollCount);
            System.out.printf("Total messages processed: %d%n", totalMessages);
            System.out.println("\nClosing consumer...");
            consumer.close();
            System.out.println("Consumer closed.");
        }

        printAnalysis();
    }

    private static void printAnalysis() {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("BASIC CONSUMER ANALYSIS");
        System.out.println("=".repeat(70));

        System.out.println("\nCONSUMER FLOW:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐");
        System.out.println("  │  subscribe  │───►│    poll     │───►│   process   │");
        System.out.println("  │   (topic)   │    │  (records)  │    │  (records)  │");
        System.out.println("  └─────────────┘    └─────────────┘    └─────────────┘");
        System.out.println("                            │                  │");
        System.out.println("                            │                  │");
        System.out.println("                            ▼                  ▼");
        System.out.println("                     ┌─────────────┐    ┌─────────────┐");
        System.out.println("                     │  wait for   │    │ auto-commit │");
        System.out.println("                     │   timeout   │    │  (5 sec)    │");
        System.out.println("                     └─────────────┘    └─────────────┘");
        System.out.println("");

        System.out.println("\nAUTO.OFFSET.RESET OPTIONS:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ┌────────────┬────────────────────────────────────────────────────┐");
        System.out.println("  │ Value      │ Behavior                                           │");
        System.out.println("  ├────────────┼────────────────────────────────────────────────────┤");
        System.out.println("  │ earliest   │ Start from first available message                 │");
        System.out.println("  │ latest     │ Start from end (only new messages)                 │");
        System.out.println("  │ none       │ Throw exception if no prior offset                 │");
        System.out.println("  └────────────┴────────────────────────────────────────────────────┘");
        System.out.println("");
        System.out.println("  When auto.offset.reset applies:");
        System.out.println("  1. Consumer group has no committed offset for partition");
        System.out.println("  2. Committed offset is invalid (deleted due to retention)");
        System.out.println("");

        System.out.println("\nAUTO-COMMIT RISK:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  Problem: Auto-commit happens on interval, not on processing");
        System.out.println("");
        System.out.println("  Timeline:");
        System.out.println("  ┌────────────────────────────────────────────────────────────────┐");
        System.out.println("  │ T=0s     T=2s        T=4s       T=5s (auto-commit)             │");
        System.out.println("  │   │        │           │              │                        │");
        System.out.println("  │   ▼        ▼           ▼              ▼                        │");
        System.out.println("  │ poll() → process → process → CRASH! ← offset committed        │");
        System.out.println("  │ msg 1    msg 1      msg 2              for msg 1-3             │");
        System.out.println("  │                     msg 3                                      │");
        System.out.println("  └────────────────────────────────────────────────────────────────┘");
        System.out.println("");
        System.out.println("  Result: msg 2 and msg 3 were committed but not fully processed!");
        System.out.println("  On restart: Consumer skips msg 2, 3 → DATA LOSS");
        System.out.println("");
        System.out.println("  Solution: Manual offset commit (Demo C03, C04)");
        System.out.println("");

        System.out.println("\nCONFIG COMPARISON:");
        System.out.println("-".repeat(70));
        System.out.println("| Config                | Demo C01   | Production        |");
        System.out.println("|-----------------------|------------|-------------------|");
        System.out.println("| enable.auto.commit    | true       | false (manual)    |");
        System.out.println("| auto.offset.reset     | earliest   | depends on use    |");
        System.out.println("| Processing guarantee  | At-most    | At-least-once     |");
        System.out.println("-".repeat(70));
    }
}