package com.example.consumer;

import com.example.Transaction;
import com.example.JsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * DEMO C03: Offset Management
 *
 * WHAT CHANGED FROM DEMO C02:
 * +---------------------------+-------------------+---------------------+
 * | Aspect                    | Demo C02          | Demo C03            |
 * +---------------------------+-------------------+---------------------+
 * | Offset visibility         | Implicit          | Explicit tracking   |
 * | Commit behavior           | Auto only         | Auto vs Manual      |
 * | Data loss scenarios       | Not shown         | Demonstrated        |
 * +---------------------------+-------------------+---------------------+
 *
 * OFFSET CONCEPT:
 *
 * ┌─────────────────────────────────────────────────────────────────────┐
 * │                    PARTITION (Sequential Log)                       │
 * │                                                                     │
 * │  Offset:  0    1    2    3    4    5    6    7    8    9    10     │
 * │         ┌───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┐             │
 * │         │M0 │M1 │M2 │M3 │M4 │M5 │M6 │M7 │M8 │M9 │M10│             │
 * │         └───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┘             │
 * │                           ▲           ▲           ▲                 │
 * │                           │           │           │                 │
 * │                    Committed    Current Pos   Latest               │
 * │                    Offset=3     (reading 5)   Offset=10            │
 * │                                                                     │
 * └─────────────────────────────────────────────────────────────────────┘
 *
 * KEY TERMS:
 * - Log End Offset (LEO): Latest message offset in partition
 * - Committed Offset: Last offset stored in __consumer_offsets
 * - Current Position: Next offset to be read by consumer
 * - Lag: LEO - Committed Offset
 *
 * OFFSET STORAGE:
 * - Stored in internal topic: __consumer_offsets
 * - Keyed by: (group_id, topic, partition)
 * - Value: (offset, metadata, timestamp)
 *
 * Banking Scenario: Ensuring no transaction is missed or duplicated
 */
public class DemoC03_OffsetManagement {

    private static final String TOPIC = "fund-transfer-events";

    public static void main(String[] args) throws InterruptedException {

        System.out.println("=".repeat(70));
        System.out.println("DEMO C03: Offset Management");
        System.out.println("=".repeat(70));
        System.out.println("Understanding offsets, commits, and data loss scenarios\n");

        // Part 1: Show offset positions
        showOffsetPositions();

        // Part 2: Auto-commit behavior
        demonstrateAutoCommit();

        // Part 3: Auto-commit data loss scenario
        demonstrateAutoCommitDataLoss();

        printAnalysis();
    }

    private static void showOffsetPositions() throws InterruptedException {
        System.out.println("-".repeat(70));
        System.out.println("PART 1: Offset Positions Explained");
        System.out.println("-".repeat(70));
        System.out.println("");

        String groupId = "offset-demo-group-" + System.currentTimeMillis();
        Properties props = createConsumerProperties(groupId, true);
        KafkaConsumer<String, Transaction> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(TOPIC));

        // First poll to get assignment
        consumer.poll(Duration.ofMillis(1000));

        Set<TopicPartition> assignment = consumer.assignment();

        if (assignment.isEmpty()) {
            System.out.println("  No partitions assigned. Make sure topic has messages.");
            consumer.close();
            return;
        }

        System.out.println("  Offset Positions per Partition:");
        System.out.println("  ┌────────────┬───────────────┬───────────────┬───────────────┬─────────┐");
        System.out.println("  │ Partition  │ Beginning     │ End (LEO)     │ Committed     │ Lag     │");
        System.out.println("  ├────────────┼───────────────┼───────────────┼───────────────┼─────────┤");

        // Get beginning and end offsets
        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(assignment);
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignment);

        for (TopicPartition tp : assignment) {
            long beginning = beginningOffsets.getOrDefault(tp, 0L);
            long end = endOffsets.getOrDefault(tp, 0L);

            // Get committed offset
            OffsetAndMetadata committed = consumer.committed(tp);
            long committedOffset = committed != null ? committed.offset() : -1;

            // Calculate lag
            long lag = committedOffset >= 0 ? end - committedOffset : end - beginning;

            System.out.printf("  │ P%-9d │ %-13d │ %-13d │ %-13s │ %-7d │%n",
                    tp.partition(),
                    beginning,
                    end,
                    committedOffset >= 0 ? String.valueOf(committedOffset) : "none",
                    lag);
        }

        System.out.println("  └────────────┴───────────────┴───────────────┴───────────────┴─────────┘");
        System.out.println("");

        // Visual representation for first partition
        TopicPartition firstPartition = assignment.iterator().next();
        long begin = beginningOffsets.get(firstPartition);
        long end = endOffsets.get(firstPartition);
        OffsetAndMetadata committed = consumer.committed(firstPartition);
        long committedOffset = committed != null ? committed.offset() : begin;

        System.out.printf("  Visual for Partition %d:%n", firstPartition.partition());
        System.out.println("");
        System.out.println("  Offset:  " + generateOffsetScale(begin, end));
        System.out.println("           " + generateOffsetMarkers(begin, end, committedOffset));
        System.out.println("");
        System.out.printf("           ▲ Beginning=%d    ▲ Committed=%d    ▲ End(LEO)=%d%n",
                begin, committedOffset, end);
        System.out.println("");

        consumer.close();
    }

    private static void demonstrateAutoCommit() throws InterruptedException {
        System.out.println("-".repeat(70));
        System.out.println("PART 2: Auto-Commit Behavior");
        System.out.println("-".repeat(70));
        System.out.println("");

        String groupId = "auto-commit-demo-" + System.currentTimeMillis();

        Properties props = createConsumerProperties(groupId, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "3000"); // 3 seconds

        KafkaConsumer<String, Transaction> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        System.out.println("  Config: enable.auto.commit=true, auto.commit.interval.ms=3000");
        System.out.println("");
        System.out.println("  Auto-commit Timeline:");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │ T=0s      T=1s      T=2s      T=3s (AUTO-COMMIT)   T=4s        │");
        System.out.println("  │   │         │         │              │               │          │");
        System.out.println("  │   ▼         ▼         ▼              ▼               ▼          │");
        System.out.println("  │ poll()   process   process     COMMIT           process        │");
        System.out.println("  │          msg1      msg2,3      offset=4         msg4,5         │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("");

        // Demonstrate auto-commit timing
        System.out.println("  Watching auto-commit in action...\n");

        TopicPartition watchPartition = null;
        long initialCommitted = -1;

        for (int i = 0; i < 5; i++) {
            ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(1000));

            if (watchPartition == null && !consumer.assignment().isEmpty()) {
                watchPartition = consumer.assignment().iterator().next();
                OffsetAndMetadata meta = consumer.committed(watchPartition);
                initialCommitted = meta != null ? meta.offset() : 0;
            }

            if (watchPartition != null) {
                OffsetAndMetadata currentCommitted = consumer.committed(watchPartition);
                long committed = currentCommitted != null ? currentCommitted.offset() : 0;
                long position = consumer.position(watchPartition);

                System.out.printf("  Poll #%d: Received %d records | Position=%d | Committed=%d%n",
                        i + 1, records.count(), position, committed);

                if (committed > initialCommitted) {
                    System.out.println("           ↑ AUTO-COMMIT occurred!");
                    initialCommitted = committed;
                }
            }

            // Simulate processing time
            Thread.sleep(800);
        }

        System.out.println("");
        consumer.close();
    }

    private static void demonstrateAutoCommitDataLoss() throws InterruptedException {
        System.out.println("-".repeat(70));
        System.out.println("PART 3: Auto-Commit Data Loss Scenario");
        System.out.println("-".repeat(70));
        System.out.println("");

        System.out.println("  SCENARIO: Crash after auto-commit but before processing complete");
        System.out.println("");
        System.out.println("  Timeline:");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │                                                                 │");
        System.out.println("  │  T=0s        T=3s             T=3.5s          T=4s             │");
        System.out.println("  │    │           │                 │              │               │");
        System.out.println("  │    ▼           ▼                 ▼              ▼               │");
        System.out.println("  │  poll()    AUTO-COMMIT      processing      CRASH!            │");
        System.out.println("  │  msg 1-5   offset=6         msg 3,4,5       ✗                 │");
        System.out.println("  │                             (not done)                         │");
        System.out.println("  │                                                                 │");
        System.out.println("  │  Messages 1,2: Processed ✓                                     │");
        System.out.println("  │  Messages 3,4,5: NOT processed but offset committed!           │");
        System.out.println("  │                                                                 │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("");
        System.out.println("  After Restart:");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │                                                                 │");
        System.out.println("  │  Consumer reads committed offset = 6                            │");
        System.out.println("  │  Resumes from offset 6 (msg 6, 7, 8...)                        │");
        System.out.println("  │                                                                 │");
        System.out.println("  │  RESULT: Messages 3, 4, 5 are LOST! Never processed!           │");
        System.out.println("  │                                                                 │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("");

        // Simulate the scenario
        System.out.println("  Simulating scenario...\n");

        String groupId = "data-loss-demo-" + System.currentTimeMillis();
        Properties props = createConsumerProperties(groupId, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "2000");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

        KafkaConsumer<String, Transaction> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        // Poll to get messages
        ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(2000));
        System.out.printf("  [Consumer 1] Polled %d messages%n", records.count());

        if (!records.isEmpty()) {
            // Process some but not all
            int processed = 0;
            for (ConsumerRecord<String, Transaction> record : records) {
                processed++;
                if (processed <= 2) {
                    System.out.printf("  [Consumer 1] Processed message at offset %d%n", record.offset());
                } else if (processed == 3) {
                    System.out.println("  [Consumer 1] Processing message 3...");
                }

                if (processed >= 3) break;
            }

            // Wait for auto-commit
            System.out.println("  [Consumer 1] Waiting for auto-commit...");
            Thread.sleep(2500);

            // Check what was committed
            TopicPartition tp = consumer.assignment().iterator().next();
            OffsetAndMetadata committed = consumer.committed(tp);
            System.out.printf("  [Consumer 1] Auto-committed offset: %d%n",
                    committed != null ? committed.offset() : 0);

            // Simulate crash
            System.out.println("  [Consumer 1] CRASH! (simulated - closing without graceful shutdown)");
            consumer.close(Duration.ofMillis(0)); // Immediate close

            // New consumer restarts
            System.out.println("");
            System.out.println("  ... Restarting consumer ...\n");

            KafkaConsumer<String, Transaction> consumer2 = new KafkaConsumer<>(
                    createConsumerProperties(groupId, true));
            consumer2.subscribe(Collections.singletonList(TOPIC));

            ConsumerRecords<String, Transaction> newRecords = consumer2.poll(Duration.ofMillis(2000));

            if (!consumer2.assignment().isEmpty()) {
                tp = consumer2.assignment().iterator().next();
                long position = consumer2.position(tp);
                System.out.printf("  [Consumer 2] Starting from position: %d%n", position);
                System.out.printf("  [Consumer 2] Messages 3,4,5 are SKIPPED - DATA LOSS!%n");
            }

            consumer2.close();
        }

        System.out.println("");
    }

    private static String generateOffsetScale(long begin, long end) {
        StringBuilder sb = new StringBuilder();
        int displayLength = Math.min(20, (int)(end - begin + 1));
        for (long i = begin; i < begin + displayLength; i++) {
            sb.append(String.format("%-4d", i));
        }
        if (end - begin >= displayLength) {
            sb.append("...");
        }
        return sb.toString();
    }

    private static String generateOffsetMarkers(long begin, long end, long committed) {
        StringBuilder sb = new StringBuilder();
        int displayLength = Math.min(20, (int)(end - begin + 1));
        for (long i = begin; i < begin + displayLength; i++) {
            if (i < committed) {
                sb.append("[■] ");  // Committed
            } else if (i == committed) {
                sb.append("[▶] ");  // Current position
            } else {
                sb.append("[□] ");  // Not yet read
            }
        }
        return sb.toString();
    }

    private static Properties createConsumerProperties(String groupId, boolean autoCommit) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        props.put("json.deserializer.type", Transaction.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));
        return props;
    }

    private static void printAnalysis() {
        System.out.println("=".repeat(70));
        System.out.println("OFFSET MANAGEMENT ANALYSIS");
        System.out.println("=".repeat(70));

        System.out.println("\nOFFSET TYPES:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ┌─────────────────────┬──────────────────────────────────────────────┐");
        System.out.println("  │ Offset Type         │ Description                                  │");
        System.out.println("  ├─────────────────────┼──────────────────────────────────────────────┤");
        System.out.println("  │ Log Start Offset    │ First available offset (may be > 0 due to   │");
        System.out.println("  │                     │ retention/compaction)                        │");
        System.out.println("  ├─────────────────────┼──────────────────────────────────────────────┤");
        System.out.println("  │ Log End Offset (LEO)│ Next offset to be written (latest + 1)      │");
        System.out.println("  ├─────────────────────┼──────────────────────────────────────────────┤");
        System.out.println("  │ Committed Offset    │ Last offset confirmed processed by consumer  │");
        System.out.println("  │                     │ (stored in __consumer_offsets)              │");
        System.out.println("  ├─────────────────────┼──────────────────────────────────────────────┤");
        System.out.println("  │ Current Position    │ Next offset to be fetched by consumer       │");
        System.out.println("  ├─────────────────────┼──────────────────────────────────────────────┤");
        System.out.println("  │ Consumer Lag        │ LEO - Committed Offset (messages behind)    │");
        System.out.println("  └─────────────────────┴──────────────────────────────────────────────┘");
        System.out.println("");

        System.out.println("\nOFFSET STORAGE (__consumer_offsets):");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  Internal Topic: __consumer_offsets (50 partitions by default)");
        System.out.println("");
        System.out.println("  Key:   (group_id, topic, partition)");
        System.out.println("  Value: (offset, leader_epoch, metadata, commit_timestamp)");
        System.out.println("");
        System.out.println("  Partition Assignment:");
        System.out.println("  partition = hash(group_id) % 50");
        System.out.println("");
        System.out.println("  Example:");
        System.out.println("  ┌──────────────────────────────────────────────────────────────────┐");
        System.out.println("  │ Key: payment-processors:fund-transfer-events:3                   │");
        System.out.println("  │ Value: offset=12345, epoch=5, metadata=\"\", timestamp=1732451234  │");
        System.out.println("  └──────────────────────────────────────────────────────────────────┘");
        System.out.println("");

        System.out.println("\nAUTO-COMMIT vs MANUAL-COMMIT:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ┌─────────────────────┬─────────────────────┬─────────────────────┐");
        System.out.println("  │ Aspect              │ Auto-Commit         │ Manual-Commit       │");
        System.out.println("  ├─────────────────────┼─────────────────────┼─────────────────────┤");
        System.out.println("  │ enable.auto.commit  │ true                │ false               │");
        System.out.println("  ├─────────────────────┼─────────────────────┼─────────────────────┤");
        System.out.println("  │ When committed      │ Every X ms          │ When you call       │");
        System.out.println("  │                     │ (default 5000)      │ commitSync/Async    │");
        System.out.println("  ├─────────────────────┼─────────────────────┼─────────────────────┤");
        System.out.println("  │ Guarantee           │ At-most-once        │ At-least-once       │");
        System.out.println("  │                     │ (may lose data)     │ (may duplicate)     │");
        System.out.println("  ├─────────────────────┼─────────────────────┼─────────────────────┤");
        System.out.println("  │ Code complexity     │ Simple              │ More control        │");
        System.out.println("  ├─────────────────────┼─────────────────────┼─────────────────────┤");
        System.out.println("  │ Use case            │ Non-critical data   │ Critical data       │");
        System.out.println("  │                     │ Metrics, logs       │ Transactions        │");
        System.out.println("  └─────────────────────┴─────────────────────┴─────────────────────┘");
        System.out.println("");

        System.out.println("\nDELIVERY SEMANTICS:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  AT-MOST-ONCE (may lose messages):");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │  1. Commit offset                                               │");
        System.out.println("  │  2. Process message      ← If crash here, message lost!        │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("");
        System.out.println("  AT-LEAST-ONCE (may duplicate messages):");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │  1. Process message                                             │");
        System.out.println("  │  2. Commit offset        ← If crash here, message reprocessed! │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("");
        System.out.println("  EXACTLY-ONCE (complex, requires transactions):");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │  1. Begin transaction                                           │");
        System.out.println("  │  2. Process message                                             │");
        System.out.println("  │  3. Write output + commit offset atomically                    │");
        System.out.println("  │  4. Commit transaction                                          │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("");

        System.out.println("\nBANKING RECOMMENDATION:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  For fund transfers, use AT-LEAST-ONCE with idempotent processing:");
        System.out.println("");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │  enable.auto.commit = false                                     │");
        System.out.println("  │                                                                 │");
        System.out.println("  │  while (running) {                                              │");
        System.out.println("  │      records = consumer.poll();                                 │");
        System.out.println("  │      for (record : records) {                                   │");
        System.out.println("  │          if (!alreadyProcessed(record.transactionId)) {         │");
        System.out.println("  │              processTransfer(record);        // Idempotent!     │");
        System.out.println("  │              markAsProcessed(record.transactionId);             │");
        System.out.println("  │          }                                                      │");
        System.out.println("  │      }                                                          │");
        System.out.println("  │      consumer.commitSync();  // Commit after processing         │");
        System.out.println("  │  }                                                              │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("");
        System.out.println("  Key: Use transaction_id as idempotency key in database");
        System.out.println("");

        System.out.println("\nCONFIG COMPARISON:");
        System.out.println("-".repeat(70));
        System.out.println("| Config                  | Auto-Commit     | Manual-Commit    |");
        System.out.println("|-------------------------|-----------------|------------------|");
        System.out.println("| enable.auto.commit      | true            | false            |");
        System.out.println("| auto.commit.interval.ms | 5000 (default)  | N/A              |");
        System.out.println("| Commit timing           | Background      | Explicit call    |");
        System.out.println("| Data loss risk          | HIGH            | LOW              |");
        System.out.println("| Duplicate risk          | LOW             | MEDIUM           |");
        System.out.println("-".repeat(70));
    }
}