package com.example.consumer;

import com.example.Transaction;
import com.example.JsonDeserializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * DEMO C07: Consumer Lag Monitoring
 *
 * LAG = Log End Offset (LEO) - Committed Offset
 *
 * LAG INTERPRETATION:
 * - Lag = 0: Consumer is caught up
 * - Lag > 0: Consumer is behind
 * - Lag increasing: Consumer can't keep up with producer
 * - Lag decreasing: Consumer is catching up
 *
 * MONITORING APPROACHES:
 * 1. Application-level: Calculate lag in consumer code
 * 2. JMX metrics: records-lag, records-lag-max
 * 3. CLI: kafka-consumer-groups.sh --describe
 * 4. External tools: Burrow, Kafka Manager
 */
public class DemoC07_ConsumerLag {

    private static final String TOPIC = "fund-transfer-events";

    public static void main(String[] args) throws InterruptedException {
        System.out.println("=".repeat(70));
        System.out.println("DEMO C07: Consumer Lag Monitoring");
        System.out.println("=".repeat(70));

        demonstrateLagCalculation();
        demonstrateSlowConsumer();
        printAnalysis();
    }

    private static void demonstrateLagCalculation() throws InterruptedException {
        System.out.println("\n" + "-".repeat(70));
        System.out.println("PART 1: Lag Calculation");
        System.out.println("-".repeat(70));

        String groupId = "lag-demo-" + System.currentTimeMillis();
        Properties props = createConsumerProperties(groupId);
        KafkaConsumer<String, Transaction> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(TOPIC));
        consumer.poll(Duration.ofMillis(100)); // Trigger assignment

        Set<TopicPartition> assignment = consumer.assignment();

        System.out.println("\nLag Calculation Formula:");
        System.out.println("  LAG = End Offset - Committed Offset");
        System.out.println("");

        // Display lag for each partition
        displayLag(consumer, assignment, "Initial State");

        // Consume some messages
        System.out.println("\nConsuming messages...");
        for (int i = 0; i < 3; i++) {
            ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(1000));
            System.out.printf("  Poll #%d: Processed %d records%n", i + 1, records.count());
            consumer.commitSync();
        }

        displayLag(consumer, assignment, "After Consuming");

        consumer.close();
    }

    private static void demonstrateSlowConsumer() throws InterruptedException {
        System.out.println("\n" + "-".repeat(70));
        System.out.println("PART 2: Slow Consumer (Lag Increases)");
        System.out.println("-".repeat(70));

        String groupId = "slow-consumer-" + System.currentTimeMillis();
        Properties props = createConsumerProperties(groupId);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10"); // Small batches

        KafkaConsumer<String, Transaction> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));
        consumer.poll(Duration.ofMillis(100));

        Set<TopicPartition> assignment = consumer.assignment();

        System.out.println("\nSimulating slow processing (100ms per record)...\n");

        for (int i = 0; i < 5; i++) {
            long startPoll = System.currentTimeMillis();
            ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(1000));

            // Slow processing
            for (ConsumerRecord<String, Transaction> record : records) {
                Thread.sleep(100); // Simulate slow processing
            }

            consumer.commitSync();
            long pollTime = System.currentTimeMillis() - startPoll;

            System.out.printf("Poll #%d: Processed %d records in %d ms%n",
                    i + 1, records.count(), pollTime);

            // Calculate and display lag
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignment);
            long totalLag = 0;
            for (TopicPartition tp : assignment) {
                OffsetAndMetadata committed = consumer.committed(tp);
                if (committed != null) {
                    long lag = endOffsets.get(tp) - committed.offset();
                    totalLag += lag;
                }
            }
            System.out.printf("         Total Lag: %d messages%n%n", totalLag);
        }

        consumer.close();
    }

    private static void displayLag(KafkaConsumer<String, Transaction> consumer,
                                   Set<TopicPartition> partitions, String phase) {
        System.out.println("\n" + phase + ":");
        System.out.println("┌────────────┬─────────────┬─────────────┬─────────────┐");
        System.out.println("│ Partition  │ End Offset  │ Committed   │ Lag         │");
        System.out.println("├────────────┼─────────────┼─────────────┼─────────────┤");

        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
        long totalLag = 0;

        for (TopicPartition tp : partitions) {
            long endOffset = endOffsets.get(tp);
            OffsetAndMetadata committed = consumer.committed(tp);
            long committedOffset = committed != null ? committed.offset() : 0;
            long lag = endOffset - committedOffset;
            totalLag += lag;

            System.out.printf("│ P%-9d │ %-11d │ %-11d │ %-11d │%n",
                    tp.partition(), endOffset, committedOffset, lag);
        }

        System.out.println("├────────────┴─────────────┴─────────────┼─────────────┤");
        System.out.printf("│ TOTAL LAG                              │ %-11d │%n", totalLag);
        System.out.println("└────────────────────────────────────────┴─────────────┘");
    }

    private static Properties createConsumerProperties(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        props.put("json.deserializer.type", Transaction.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return props;
    }

    private static void printAnalysis() {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("CONSUMER LAG ANALYSIS");
        System.out.println("=".repeat(70));

        System.out.println("\nLAG METRICS:");
        System.out.println("-".repeat(70));
        System.out.println("┌──────────────────────────┬──────────────────────────────────────┐");
        System.out.println("│ Metric                   │ Meaning                              │");
        System.out.println("├──────────────────────────┼──────────────────────────────────────┤");
        System.out.println("│ records-lag              │ Current lag per partition            │");
        System.out.println("├──────────────────────────┼──────────────────────────────────────┤");
        System.out.println("│ records-lag-max          │ Max lag across all partitions        │");
        System.out.println("├──────────────────────────┼──────────────────────────────────────┤");
        System.out.println("│ records-lag-avg          │ Average lag across partitions        │");
        System.out.println("└──────────────────────────┴──────────────────────────────────────┘");

        System.out.println("\n\nLAG THRESHOLDS (Banking):");
        System.out.println("-".repeat(70));
        System.out.println("  Lag < 100 messages:     GREEN  (healthy)");
        System.out.println("  Lag 100-1000:           YELLOW (monitor)");
        System.out.println("  Lag > 1000:             ORANGE (investigate)");
        System.out.println("  Lag > 10,000:           RED    (critical - scale up!)");
        System.out.println("");

        System.out.println("\nMONITORING VIA CLI:");
        System.out.println("-".repeat(70));
        System.out.println("kafka-consumer-groups.sh --bootstrap-server localhost:9092 \\");
        System.out.println("  --group payment-processors --describe");
        System.out.println("");
        System.out.println("Output:");
        System.out.println("GROUP           TOPIC               PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG");
        System.out.println("payment-proc    fund-transfer       0          1500            1523            23");
        System.out.println("payment-proc    fund-transfer       1          1480            1498            18");
        System.out.println("");

        System.out.println("\nREDUCING LAG:");
        System.out.println("-".repeat(70));
        System.out.println("1. ADD MORE CONSUMERS");
        System.out.println("   - Scale horizontally (up to # of partitions)");
        System.out.println("   - Rebalance distributes load");
        System.out.println("");
        System.out.println("2. OPTIMIZE PROCESSING");
        System.out.println("   - Profile slow code paths");
        System.out.println("   - Batch database operations");
        System.out.println("   - Use connection pooling");
        System.out.println("");
        System.out.println("3. INCREASE PARTITIONS");
        System.out.println("   - More partitions = more parallelism");
        System.out.println("   - Requires careful planning (can't decrease)");
        System.out.println("");
        System.out.println("4. TUNE CONSUMER CONFIGS");
        System.out.println("   - Increase max.poll.records (larger batches)");
        System.out.println("   - Increase fetch.min.bytes (wait for more data)");
        System.out.println("   - Decrease fetch.max.wait.ms (don't wait long)");
        System.out.println("-".repeat(70));
    }
}