package com.example.consumer;

import com.example.Transaction;
import com.example.JsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DEMO C02: Consumer with Group
 *
 * WHAT CHANGED FROM DEMO C01:
 * +---------------------------+-------------------+---------------------+
 * | Aspect                    | Demo C01          | Demo C02            |
 * +---------------------------+-------------------+---------------------+
 * | Consumers                 | Single            | Multiple            |
 * | Partition assignment      | Not visible       | Visualized          |
 * | Group coordination        | Implicit          | Explicit demo       |
 * +---------------------------+-------------------+---------------------+
 *
 * CONSUMER GROUP CONCEPT:
 *
 * ┌─────────────────────────────────────────────────────────────────────┐
 * │                        CONSUMER GROUP                               │
 * │                     group.id = "payment-processors"                 │
 * │                                                                     │
 * │  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐               │
 * │  │ Consumer 1  │   │ Consumer 2  │   │ Consumer 3  │               │
 * │  │ Partitions: │   │ Partitions: │   │ Partitions: │               │
 * │  │   0, 1      │   │   2, 3      │   │   4, 5      │               │
 * │  └─────────────┘   └─────────────┘   └─────────────┘               │
 * │                                                                     │
 * └─────────────────────────────────────────────────────────────────────┘
 *                                │
 *                                ▼
 * ┌─────────────────────────────────────────────────────────────────────┐
 * │                      KAFKA TOPIC                                    │
 * │              fund-transfer-events (6 partitions)                    │
 * │                                                                     │
 * │  ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐                        │
 * │  │ P0 │ │ P1 │ │ P2 │ │ P3 │ │ P4 │ │ P5 │                        │
 * │  └────┘ └────┘ └────┘ └────┘ └────┘ └────┘                        │
 * │                                                                     │
 * └─────────────────────────────────────────────────────────────────────┘
 *
 * KEY RULES:
 * 1. Each partition is assigned to exactly ONE consumer in the group
 * 2. One consumer can have multiple partitions
 * 3. If consumers > partitions, some consumers will be idle
 * 4. If consumers < partitions, some consumers get multiple partitions
 *
 * GROUP COORDINATOR:
 * - One broker acts as Group Coordinator for each group
 * - Manages membership and partition assignment
 * - Handles heartbeats and rebalancing
 *
 * Banking Scenario: Multiple payment processors sharing load
 */
public class DemoC02_ConsumerGroup {

    private static final String TOPIC = "fund-transfer-events";
    private static final String GROUP_ID = "payment-processors";
    private static final AtomicBoolean running = new AtomicBoolean(true);
    private static final AtomicInteger totalMessagesProcessed = new AtomicInteger(0);

    public static void main(String[] args) throws InterruptedException {

        System.out.println("=".repeat(70));
        System.out.println("DEMO C02: Consumer with Group");
        System.out.println("=".repeat(70));
        System.out.println("Multiple consumers sharing partitions in a group\n");

        // Part 1: Show single consumer getting all partitions
        runSingleConsumerDemo();

        // Part 2: Show multiple consumers sharing partitions
        runMultipleConsumersDemo(3);

        printAnalysis();
    }

    private static void runSingleConsumerDemo() throws InterruptedException {
        System.out.println("-".repeat(70));
        System.out.println("PART 1: Single Consumer (gets all partitions)");
        System.out.println("-".repeat(70));
        System.out.println("");

        Properties props = createConsumerProperties("single-consumer-group", "consumer-1");
        KafkaConsumer<String, Transaction> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(TOPIC));

        // First poll triggers partition assignment
        System.out.println("  Subscribing and waiting for partition assignment...\n");
        consumer.poll(Duration.ofMillis(1000));

        // Show assigned partitions
        Set<TopicPartition> assignment = consumer.assignment();
        System.out.printf("  Consumer assigned %d partitions:%n", assignment.size());
        assignment.forEach(tp ->
                System.out.printf("    • %s-%d%n", tp.topic(), tp.partition()));

        System.out.println("\n  Single consumer gets ALL partitions!\n");

        consumer.close();
    }

    private static void runMultipleConsumersDemo(int numConsumers) throws InterruptedException {
        System.out.println("-".repeat(70));
        System.out.printf("PART 2: Multiple Consumers (%d consumers sharing partitions)%n", numConsumers);
        System.out.println("-".repeat(70));
        System.out.println("");

        ExecutorService executor = Executors.newFixedThreadPool(numConsumers);
        CountDownLatch startLatch = new CountDownLatch(numConsumers);
        CountDownLatch assignmentLatch = new CountDownLatch(numConsumers);

        running.set(true);
        totalMessagesProcessed.set(0);

        System.out.printf("  Starting %d consumers in group '%s'...%n%n", numConsumers, GROUP_ID);

        // Start multiple consumers
        for (int i = 1; i <= numConsumers; i++) {
            final int consumerId = i;
            executor.submit(() -> runConsumer(consumerId, startLatch, assignmentLatch));
        }

        // Wait for all consumers to get assignments
        assignmentLatch.await(30, TimeUnit.SECONDS);

        System.out.println("\n  All consumers received partition assignments!");
        System.out.println("  Processing messages for 10 seconds...\n");

        // Let consumers process for a while
        Thread.sleep(10000);

        // Stop consumers
        running.set(false);
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        System.out.printf("%n  Total messages processed by group: %d%n%n", totalMessagesProcessed.get());
    }

    private static void runConsumer(int consumerId, CountDownLatch startLatch, CountDownLatch assignmentLatch) {
        String consumerName = "consumer-" + consumerId;

        Properties props = createConsumerProperties(GROUP_ID, consumerName);
        KafkaConsumer<String, Transaction> consumer = new KafkaConsumer<>(props);

        try {
            consumer.subscribe(Collections.singletonList(TOPIC));
            startLatch.countDown();

            // First poll to trigger assignment
            consumer.poll(Duration.ofMillis(100));

            // Show assignment
            Set<TopicPartition> assignment = consumer.assignment();
            synchronized (System.out) {
                System.out.printf("  [%s] Assigned partitions: ", consumerName);
                if (assignment.isEmpty()) {
                    System.out.println("NONE (idle)");
                } else {
                    assignment.forEach(tp -> System.out.printf("P%d ", tp.partition()));
                    System.out.println();
                }
            }

            assignmentLatch.countDown();

            // Process messages
            int localCount = 0;
            while (running.get()) {
                ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, Transaction> record : records) {
                    localCount++;
                    totalMessagesProcessed.incrementAndGet();

                    if (localCount <= 2) {
                        synchronized (System.out) {
                            System.out.printf("  [%s] Processed: P%d-Offset%d, Key=%s%n",
                                    consumerName, record.partition(), record.offset(), record.key());
                        }
                    }
                }
            }

            synchronized (System.out) {
                System.out.printf("  [%s] Shutting down. Processed %d messages.%n", consumerName, localCount);
            }

        } finally {
            consumer.close();
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
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        // Faster rebalance for demo
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");

        return props;
    }

    private static void printAnalysis() {
        System.out.println("=".repeat(70));
        System.out.println("CONSUMER GROUP ANALYSIS");
        System.out.println("=".repeat(70));

        System.out.println("\nPARTITION ASSIGNMENT SCENARIOS:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  SCENARIO 1: 6 Partitions, 1 Consumer");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │                      Consumer 1                                 │");
        System.out.println("  │               [P0] [P1] [P2] [P3] [P4] [P5]                    │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("  One consumer handles ALL partitions");
        System.out.println("");
        System.out.println("  SCENARIO 2: 6 Partitions, 3 Consumers");
        System.out.println("  ┌───────────────────┐ ┌───────────────────┐ ┌───────────────────┐");
        System.out.println("  │    Consumer 1     │ │    Consumer 2     │ │    Consumer 3     │");
        System.out.println("  │    [P0] [P1]      │ │    [P2] [P3]      │ │    [P4] [P5]      │");
        System.out.println("  └───────────────────┘ └───────────────────┘ └───────────────────┘");
        System.out.println("  Even distribution: 2 partitions per consumer");
        System.out.println("");
        System.out.println("  SCENARIO 3: 6 Partitions, 6 Consumers");
        System.out.println("  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐");
        System.out.println("  │ C1  │ │ C2  │ │ C3  │ │ C4  │ │ C5  │ │ C6  │");
        System.out.println("  │[P0] │ │[P1] │ │[P2] │ │[P3] │ │[P4] │ │[P5] │");
        System.out.println("  └─────┘ └─────┘ └─────┘ └─────┘ └─────┘ └─────┘");
        System.out.println("  Perfect: 1 partition per consumer");
        System.out.println("");
        System.out.println("  SCENARIO 4: 6 Partitions, 8 Consumers");
        System.out.println("  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐");
        System.out.println("  │ C1  │ │ C2  │ │ C3  │ │ C4  │ │ C5  │ │ C6  │ │ C7  │ │ C8  │");
        System.out.println("  │[P0] │ │[P1] │ │[P2] │ │[P3] │ │[P4] │ │[P5] │ │IDLE │ │IDLE │");
        System.out.println("  └─────┘ └─────┘ └─────┘ └─────┘ └─────┘ └─────┘ └─────┘ └─────┘");
        System.out.println("  2 consumers are IDLE (no partitions assigned)");
        System.out.println("");

        System.out.println("\nGROUP COORDINATOR:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │                         KAFKA CLUSTER                           │");
        System.out.println("  │                                                                 │");
        System.out.println("  │   ┌──────────┐   ┌──────────┐   ┌──────────┐                   │");
        System.out.println("  │   │ Broker 1 │   │ Broker 2 │   │ Broker 3 │                   │");
        System.out.println("  │   │          │   │ (GROUP   │   │          │                   │");
        System.out.println("  │   │          │   │COORDINATOR│   │          │                   │");
        System.out.println("  │   │          │   │   ★)     │   │          │                   │");
        System.out.println("  │   └──────────┘   └──────────┘   └──────────┘                   │");
        System.out.println("  │                        │                                        │");
        System.out.println("  └────────────────────────┼────────────────────────────────────────┘");
        System.out.println("                           │");
        System.out.println("               ┌───────────┴───────────┐");
        System.out.println("               │    Responsibilities:  │");
        System.out.println("               │    • Member management│");
        System.out.println("               │    • Partition assign │");
        System.out.println("               │    • Heartbeat track  │");
        System.out.println("               │    • Rebalance trigger│");
        System.out.println("               └───────────────────────┘");
        System.out.println("");

        System.out.println("\nPARTITION ASSIGNMENT STRATEGIES:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ┌─────────────────────┬───────────────────────────────────────────┐");
        System.out.println("  │ Strategy            │ Description                               │");
        System.out.println("  ├─────────────────────┼───────────────────────────────────────────┤");
        System.out.println("  │ RangeAssignor       │ Assigns partition ranges per topic        │");
        System.out.println("  │ (default)           │ May cause imbalance across topics         │");
        System.out.println("  ├─────────────────────┼───────────────────────────────────────────┤");
        System.out.println("  │ RoundRobinAssignor  │ Round-robin across all partitions         │");
        System.out.println("  │                     │ Better balance, more reassignment         │");
        System.out.println("  ├─────────────────────┼───────────────────────────────────────────┤");
        System.out.println("  │ StickyAssignor      │ Balanced + sticky (minimize movement)     │");
        System.out.println("  │                     │ Best for stateful consumers               │");
        System.out.println("  ├─────────────────────┼───────────────────────────────────────────┤");
        System.out.println("  │ CooperativeSticky   │ Incremental rebalance (no stop-the-world) │");
        System.out.println("  │ Assignor            │ Recommended for Kafka 2.4+                │");
        System.out.println("  └─────────────────────┴───────────────────────────────────────────┘");
        System.out.println("");
        System.out.println("  Config: partition.assignment.strategy=");
        System.out.println("          org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
        System.out.println("");

        System.out.println("\nMULTIPLE CONSUMER GROUPS:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  Same topic can be consumed by MULTIPLE groups independently:");
        System.out.println("");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │              fund-transfer-events (6 partitions)               │");
        System.out.println("  │                                                                 │");
        System.out.println("  │  [P0] [P1] [P2] [P3] [P4] [P5]                                 │");
        System.out.println("  └───────────────┬───────────────────────┬─────────────────────────┘");
        System.out.println("                  │                       │");
        System.out.println("         ┌────────┴────────┐     ┌────────┴────────┐");
        System.out.println("         │ GROUP A         │     │ GROUP B         │");
        System.out.println("         │ payment-proc    │     │ fraud-detector  │");
        System.out.println("         │                 │     │                 │");
        System.out.println("         │ C1[P0,P1]       │     │ C1[P0,P1,P2]    │");
        System.out.println("         │ C2[P2,P3]       │     │ C2[P3,P4,P5]    │");
        System.out.println("         │ C3[P4,P5]       │     │                 │");
        System.out.println("         └─────────────────┘     └─────────────────┘");
        System.out.println("");
        System.out.println("  Each group:");
        System.out.println("  • Has independent offsets");
        System.out.println("  • Reads ALL messages");
        System.out.println("  • Processes at its own pace");
        System.out.println("");

        System.out.println("\nCONSUMER GROUP COMMANDS:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  # List all consumer groups");
        System.out.println("  kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list");
        System.out.println("");
        System.out.println("  # Describe a consumer group");
        System.out.println("  kafka-consumer-groups.sh --bootstrap-server localhost:9092 \\");
        System.out.println("    --group payment-processors --describe");
        System.out.println("");
        System.out.println("  # Show group members");
        System.out.println("  kafka-consumer-groups.sh --bootstrap-server localhost:9092 \\");
        System.out.println("    --group payment-processors --describe --members");
        System.out.println("");

        System.out.println("\nBANKING EXAMPLE:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  Topic: fund-transfer-events (6 partitions)");
        System.out.println("");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │ GROUP: transaction-processors                                   │");
        System.out.println("  │ Purpose: Process fund transfers                                 │");
        System.out.println("  │ Consumers: 3 (one per availability zone)                        │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │ GROUP: fraud-detection                                          │");
        System.out.println("  │ Purpose: Real-time fraud analysis                               │");
        System.out.println("  │ Consumers: 2 (ML model instances)                               │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │ GROUP: audit-logger                                             │");
        System.out.println("  │ Purpose: Compliance audit logging                               │");
        System.out.println("  │ Consumers: 1 (single writer to audit DB)                        │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("");

        System.out.println("\nCONFIG COMPARISON:");
        System.out.println("-".repeat(70));
        System.out.println("| Config                      | Demo C01    | Demo C02     |");
        System.out.println("|-----------------------------|-------------|--------------|");
        System.out.println("| Consumers                   | 1           | Multiple     |");
        System.out.println("| Partition visibility        | Implicit    | Explicit     |");
        System.out.println("| Load sharing                | No          | Yes          |");
        System.out.println("| Fault tolerance             | None        | Auto-failover|");
        System.out.println("-".repeat(70));
    }
}