package com.example.consumer;

import com.example.Transaction;
import com.example.JsonDeserializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * DEMO C05: Consumer Rebalancing
 *
 * REBALANCING TRIGGERS:
 * 1. Consumer joins group (new consumer)
 * 2. Consumer leaves group (close/crash)
 * 3. Consumer timeout (missed heartbeats)
 * 4. Partition count changes
 *
 * REBALANCE LISTENER:
 * - onPartitionsRevoked(): Before losing partitions (commit offsets!)
 * - onPartitionsAssigned(): After getting partitions (initialize state)
 */
public class DemoC05_ConsumerRebalancing {

    private static final String TOPIC = "fund-transfer-events";
    private static final AtomicBoolean running = new AtomicBoolean(true);

    public static void main(String[] args) throws InterruptedException {
        System.out.println("=".repeat(70));
        System.out.println("DEMO C05: Consumer Rebalancing");
        System.out.println("=".repeat(70));

        demonstrateRebalanceScenario();
        printAnalysis();
    }

    private static void demonstrateRebalanceScenario() throws InterruptedException {
        System.out.println("\nSCENARIO: Start 2 consumers, add 3rd (triggers rebalance), remove one\n");

        String groupId = "rebalance-demo-" + System.currentTimeMillis();
        ExecutorService executor = Executors.newFixedThreadPool(3);
        CountDownLatch startLatch = new CountDownLatch(2);

        running.set(true);

        // Start Consumer 1 & 2
        System.out.println("Starting Consumer 1 & 2...\n");
        executor.submit(() -> runConsumerWithListener("C1", groupId, startLatch));
        executor.submit(() -> runConsumerWithListener("C2", groupId, startLatch));

        startLatch.await(10, TimeUnit.SECONDS);
        Thread.sleep(3000);

        // Add Consumer 3 (triggers rebalance)
        System.out.println("\n>>> Adding Consumer 3 (REBALANCE!) <<<\n");
        CountDownLatch c3Latch = new CountDownLatch(1);
        executor.submit(() -> runConsumerWithListener("C3", groupId, c3Latch));

        c3Latch.await(10, TimeUnit.SECONDS);
        Thread.sleep(5000);

        // Stop all
        System.out.println("\n>>> Stopping all consumers <<<\n");
        running.set(false);
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    private static void runConsumerWithListener(String name, String groupId, CountDownLatch latch) {
        Properties props = createConsumerProperties(groupId, name);
        KafkaConsumer<String, Transaction> consumer = new KafkaConsumer<>(props);

        // Rebalance listener with state management
        Map<TopicPartition, Long> currentOffsets = new HashMap<>();

        ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                synchronized (System.out) {
                    System.out.printf("[%s] REVOKED partitions: ", name);
                    partitions.forEach(tp -> System.out.printf("P%d ", tp.partition()));
                    System.out.println();

                    // CRITICAL: Commit before losing partitions
                    if (!currentOffsets.isEmpty()) {
                        System.out.printf("[%s] Committing offsets before revocation%n", name);
                        Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
                        currentOffsets.forEach((tp, offset) ->
                                toCommit.put(tp, new OffsetAndMetadata(offset)));
                        consumer.commitSync(toCommit);
                    }
                    currentOffsets.clear();
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                synchronized (System.out) {
                    System.out.printf("[%s] ASSIGNED partitions: ", name);
                    partitions.forEach(tp -> System.out.printf("P%d ", tp.partition()));
                    System.out.println();
                }
            }
        };

        try {
            consumer.subscribe(Collections.singletonList(TOPIC), listener);
            latch.countDown();

            int pollCount = 0;
            while (running.get() && pollCount < 20) {
                ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, Transaction> record : records) {
                    // Track offsets
                    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                    currentOffsets.put(tp, record.offset() + 1);
                }

                if (!records.isEmpty() && pollCount % 5 == 0) {
                    synchronized (System.out) {
                        System.out.printf("[%s] Processed %d records from %s%n",
                                name, records.count(), consumer.assignment());
                    }
                }
                pollCount++;
            }
        } finally {
            consumer.close();
            synchronized (System.out) {
                System.out.printf("[%s] Closed%n", name);
            }
        }
    }

    private static Properties createConsumerProperties(String groupId, String clientId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        props.put("json.deserializer.type", Transaction.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
        return props;
    }

    private static void printAnalysis() {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("REBALANCING ANALYSIS");
        System.out.println("=".repeat(70));

        System.out.println("\nREBALANCE TYPES:");
        System.out.println("-".repeat(70));
        System.out.println("EAGER REBALANCING (default, stop-the-world):");
        System.out.println("  1. All consumers STOP consuming");
        System.out.println("  2. All partitions REVOKED from all consumers");
        System.out.println("  3. New assignment calculated");
        System.out.println("  4. Partitions ASSIGNED to consumers");
        System.out.println("  5. Consumers RESUME");
        System.out.println("  Impact: ENTIRE GROUP stops during rebalance!");
        System.out.println("");
        System.out.println("COOPERATIVE REBALANCING (incremental, Kafka 2.4+):");
        System.out.println("  1. Only AFFECTED partitions revoked");
        System.out.println("  2. Other consumers CONTINUE consuming");
        System.out.println("  3. Revoked partitions reassigned");
        System.out.println("  Impact: Minimal disruption!");
        System.out.println("");

        System.out.println("\nREBALANCE LISTENER PATTERN:");
        System.out.println("-".repeat(70));
        System.out.println("ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {");
        System.out.println("    @Override");
        System.out.println("    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {");
        System.out.println("        // CRITICAL: Commit offsets before losing partitions");
        System.out.println("        consumer.commitSync(currentOffsets);");
        System.out.println("        // Save any in-memory state");
        System.out.println("        saveState();");
        System.out.println("    }");
        System.out.println("    ");
        System.out.println("    @Override");
        System.out.println("    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {");
        System.out.println("        // Load state for new partitions");
        System.out.println("        loadState(partitions);");
        System.out.println("        // Optionally seek to specific offsets");
        System.out.println("    }");
        System.out.println("};");
        System.out.println("");

        System.out.println("\nREBALANCE CONFIGS:");
        System.out.println("-".repeat(70));
        System.out.println("┌──────────────────────────────┬─────────────┬─────────────────────────┐");
        System.out.println("│ Config                       │ Default     │ Impact                  │");
        System.out.println("├──────────────────────────────┼─────────────┼─────────────────────────┤");
        System.out.println("│ session.timeout.ms           │ 45000       │ Time before consumer    │");
        System.out.println("│                              │             │ considered dead         │");
        System.out.println("├──────────────────────────────┼─────────────┼─────────────────────────┤");
        System.out.println("│ heartbeat.interval.ms        │ 3000        │ Heartbeat frequency     │");
        System.out.println("│                              │             │ (< session.timeout/3)   │");
        System.out.println("├──────────────────────────────┼─────────────┼─────────────────────────┤");
        System.out.println("│ max.poll.interval.ms         │ 300000      │ Max time between polls  │");
        System.out.println("│                              │ (5 min)     │ before rebalance        │");
        System.out.println("├──────────────────────────────┼─────────────┼─────────────────────────┤");
        System.out.println("│ partition.assignment.strategy│ Range       │ RangeAssignor,          │");
        System.out.println("│                              │             │ RoundRobin, Sticky,     │");
        System.out.println("│                              │             │ CooperativeSticky       │");
        System.out.println("└──────────────────────────────┴─────────────┴─────────────────────────┘");
        System.out.println("");

        System.out.println("\nBANKING BEST PRACTICE:");
        System.out.println("-".repeat(70));
        System.out.println("For production payment processing:");
        System.out.println("1. ALWAYS use ConsumerRebalanceListener");
        System.out.println("2. COMMIT offsets in onPartitionsRevoked()");
        System.out.println("3. Use CooperativeStickyAssignor (minimize disruption)");
        System.out.println("4. Keep poll() fast (< max.poll.interval.ms)");
        System.out.println("5. Handle state persistence (database, cache)");
        System.out.println("");
        System.out.println("Config:");
        System.out.println("  partition.assignment.strategy=org.apache.kafka.clients.");
        System.out.println("    consumer.CooperativeStickyAssignor");
        System.out.println("  session.timeout.ms=30000");
        System.out.println("  heartbeat.interval.ms=10000");
        System.out.println("  max.poll.interval.ms=300000");
        System.out.println("-".repeat(70));
    }
}