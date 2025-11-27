package com.example.consumer;

import com.example.Transaction;
import com.example.JsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * DEMO C04: Manual Offset Commit
 *
 * WHAT CHANGED FROM DEMO C03:
 * +---------------------------+-------------------+---------------------+
 * | Aspect                    | Demo C03          | Demo C04            |
 * +---------------------------+-------------------+---------------------+
 * | enable.auto.commit        | true              | false               |
 * | Commit timing             | Background timer  | Explicit call       |
 * | Commit methods            | None              | commitSync/Async    |
 * | Control                   | Low               | Full                |
 * +---------------------------+-------------------+---------------------+
 *
 * COMMIT METHODS:
 *
 * 1. commitSync()
 *    - Blocks until commit completes
 *    - Retries on failure
 *    - Safest option
 *
 * 2. commitAsync()
 *    - Non-blocking, returns immediately
 *    - No retry (may fail silently)
 *    - Higher throughput
 *
 * 3. commitSync(Map<TopicPartition, OffsetAndMetadata>)
 *    - Commit specific offsets
 *    - Fine-grained control
 *    - Per-record commit possible
 *
 * COMMIT STRATEGIES:
 *
 * ┌─────────────────────────────────────────────────────────────────────┐
 * │ STRATEGY 1: Commit after each poll (batch commit)                  │
 * │                                                                     │
 * │   poll() → process all → commitSync()                              │
 * │                                                                     │
 * │   Pros: Simple, one commit per batch                                │
 * │   Cons: Reprocess entire batch on failure                          │
 * └─────────────────────────────────────────────────────────────────────┘
 *
 * ┌─────────────────────────────────────────────────────────────────────┐
 * │ STRATEGY 2: Commit after each record                               │
 * │                                                                     │
 * │   poll() → for each record: process → commitSync(offset)           │
 * │                                                                     │
 * │   Pros: Minimal reprocessing on failure                             │
 * │   Cons: High overhead, slow                                        │
 * └─────────────────────────────────────────────────────────────────────┘
 *
 * ┌─────────────────────────────────────────────────────────────────────┐
 * │ STRATEGY 3: Commit every N records                                 │
 * │                                                                     │
 * │   poll() → process N → commitSync() → process N → commitSync()     │
 * │                                                                     │
 * │   Pros: Balance between safety and performance                      │
 * │   Cons: May reprocess up to N-1 records                            │
 * └─────────────────────────────────────────────────────────────────────┘
 *
 * Banking Scenario: At-least-once processing for fund transfers
 */
public class DemoC04_ManualOffsetCommit {

    private static final String TOPIC = "fund-transfer-events";

    public static void main(String[] args) throws InterruptedException {

        System.out.println("=".repeat(70));
        System.out.println("DEMO C04: Manual Offset Commit");
        System.out.println("=".repeat(70));
        System.out.println("Explicit control over offset commits\n");

        // Part 1: commitSync after batch
        demonstrateCommitSyncBatch();

        // Part 2: commitAsync with callback
        demonstrateCommitAsync();

        // Part 3: Commit per record
        demonstrateCommitPerRecord();

        // Part 4: Commit every N records
        demonstrateCommitEveryN();

        // Part 5: Combined strategy (async + sync on shutdown)
        demonstrateCombinedStrategy();

        printAnalysis();
    }

    private static void demonstrateCommitSyncBatch() throws InterruptedException {
        System.out.println("-".repeat(70));
        System.out.println("PART 1: commitSync() after Batch");
        System.out.println("-".repeat(70));
        System.out.println("");

        String groupId = "commit-sync-batch-" + System.currentTimeMillis();
        Properties props = createConsumerProperties(groupId);
        KafkaConsumer<String, Transaction> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(TOPIC));

        System.out.println("  Strategy: Process all records, then commitSync()");
        System.out.println("");
        System.out.println("  Flow:");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │ poll() → process(msg1) → process(msg2) → ... → commitSync()   │");
        System.out.println("  │                                                    (blocking)   │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("");

        int totalProcessed = 0;
        int pollCount = 0;

        while (pollCount < 3) {
            ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(1000));
            pollCount++;

            if (records.isEmpty()) {
                System.out.printf("  Poll #%d: No records%n", pollCount);
                continue;
            }

            System.out.printf("  Poll #%d: Processing %d records...%n", pollCount, records.count());

            // Process all records
            for (ConsumerRecord<String, Transaction> record : records) {
                processRecord(record);
                totalProcessed++;
            }

            // Commit AFTER processing entire batch
            long startCommit = System.currentTimeMillis();
            consumer.commitSync();  // BLOCKING
            long commitTime = System.currentTimeMillis() - startCommit;

            System.out.printf("  Poll #%d: commitSync() completed in %d ms%n%n", pollCount, commitTime);
        }

        System.out.printf("  Total processed: %d records%n%n", totalProcessed);
        consumer.close();
    }

    private static void demonstrateCommitAsync() throws InterruptedException {
        System.out.println("-".repeat(70));
        System.out.println("PART 2: commitAsync() with Callback");
        System.out.println("-".repeat(70));
        System.out.println("");

        String groupId = "commit-async-" + System.currentTimeMillis();
        Properties props = createConsumerProperties(groupId);
        KafkaConsumer<String, Transaction> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(TOPIC));

        System.out.println("  Strategy: Non-blocking commit with callback for monitoring");
        System.out.println("");
        System.out.println("  Flow:");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │ poll() → process all → commitAsync(callback) → continue...     │");
        System.out.println("  │                              │                                  │");
        System.out.println("  │                              └─► callback invoked later        │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("");

        // Callback for async commit
        OffsetCommitCallback callback = (offsets, exception) -> {
            if (exception != null) {
                System.err.printf("  [CALLBACK] Commit FAILED: %s%n", exception.getMessage());
            } else {
                System.out.printf("  [CALLBACK] Commit SUCCESS: %s%n", formatOffsets(offsets));
            }
        };

        int pollCount = 0;

        while (pollCount < 3) {
            ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(1000));
            pollCount++;

            if (records.isEmpty()) {
                continue;
            }

            System.out.printf("  Poll #%d: Processing %d records...%n", pollCount, records.count());

            for (ConsumerRecord<String, Transaction> record : records) {
                processRecord(record);
            }

            // Async commit - returns immediately
            System.out.printf("  Poll #%d: commitAsync() called (non-blocking)%n", pollCount);
            consumer.commitAsync(callback);

            // Continue processing while commit happens in background
            System.out.printf("  Poll #%d: Continuing to next poll immediately...%n%n", pollCount);
        }

        // Important: Final sync commit before closing
        System.out.println("  Final commitSync() before close (ensure all committed)...");
        consumer.commitSync();
        System.out.println("  Done.\n");

        consumer.close();
    }

    private static void demonstrateCommitPerRecord() throws InterruptedException {
        System.out.println("-".repeat(70));
        System.out.println("PART 3: Commit per Record (Fine-grained)");
        System.out.println("-".repeat(70));
        System.out.println("");

        String groupId = "commit-per-record-" + System.currentTimeMillis();
        Properties props = createConsumerProperties(groupId);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5");

        KafkaConsumer<String, Transaction> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        System.out.println("  Strategy: Commit after EACH record processed");
        System.out.println("");
        System.out.println("  Flow:");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │ poll() → process(msg1) → commit(1)                             │");
        System.out.println("  │        → process(msg2) → commit(2)                             │");
        System.out.println("  │        → process(msg3) → commit(3)                             │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("");
        System.out.println("  ⚠ Warning: High overhead, use only for critical processing\n");

        ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(2000));

        if (!records.isEmpty()) {
            System.out.printf("  Received %d records, committing each individually:%n%n", records.count());

            long totalCommitTime = 0;
            int count = 0;

            for (ConsumerRecord<String, Transaction> record : records) {
                count++;

                // Process record
                processRecord(record);

                // Commit THIS specific offset
                Map<TopicPartition, OffsetAndMetadata> offsetToCommit = new HashMap<>();
                offsetToCommit.put(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1)  // +1 = next offset to read
                );

                long startCommit = System.currentTimeMillis();
                consumer.commitSync(offsetToCommit);
                long commitTime = System.currentTimeMillis() - startCommit;
                totalCommitTime += commitTime;

                System.out.printf("    Record %d: P%d-Offset%d → Committed (offset=%d) in %d ms%n",
                        count, record.partition(), record.offset(), record.offset() + 1, commitTime);
            }

            System.out.printf("%n  Total commit time: %d ms (avg %.1f ms per commit)%n%n",
                    totalCommitTime, totalCommitTime / (double) count);
        }

        consumer.close();
    }

    private static void demonstrateCommitEveryN() throws InterruptedException {
        System.out.println("-".repeat(70));
        System.out.println("PART 4: Commit every N Records (Balanced)");
        System.out.println("-".repeat(70));
        System.out.println("");

        String groupId = "commit-every-n-" + System.currentTimeMillis();
        Properties props = createConsumerProperties(groupId);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        KafkaConsumer<String, Transaction> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        final int COMMIT_INTERVAL = 10;  // Commit every 10 records

        System.out.printf("  Strategy: Commit every %d records%n", COMMIT_INTERVAL);
        System.out.println("");
        System.out.println("  Flow:");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │ poll() → process 1-10 → commit                                 │");
        System.out.println("  │        → process 11-20 → commit                                │");
        System.out.println("  │        → process 21-30 → commit                                │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("");

        ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(2000));

        if (!records.isEmpty()) {
            System.out.printf("  Received %d records, committing every %d:%n%n",
                    records.count(), COMMIT_INTERVAL);

            int processedSinceCommit = 0;
            int totalProcessed = 0;
            int commitCount = 0;
            Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

            for (ConsumerRecord<String, Transaction> record : records) {
                // Process record
                processRecord(record);
                totalProcessed++;
                processedSinceCommit++;

                // Track current offset for each partition
                currentOffsets.put(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1)
                );

                // Commit every N records
                if (processedSinceCommit >= COMMIT_INTERVAL) {
                    consumer.commitSync(currentOffsets);
                    commitCount++;
                    System.out.printf("    Commit #%d after record %d: %s%n",
                            commitCount, totalProcessed, formatOffsets(currentOffsets));
                    processedSinceCommit = 0;
                }
            }

            // Commit remaining
            if (processedSinceCommit > 0) {
                consumer.commitSync(currentOffsets);
                commitCount++;
                System.out.printf("    Commit #%d (final) after record %d: %s%n",
                        commitCount, totalProcessed, formatOffsets(currentOffsets));
            }

            System.out.printf("%n  Total: %d records, %d commits%n%n", totalProcessed, commitCount);
        }

        consumer.close();
    }

    private static void demonstrateCombinedStrategy() throws InterruptedException {
        System.out.println("-".repeat(70));
        System.out.println("PART 5: Combined Strategy (Async + Sync on Shutdown)");
        System.out.println("-".repeat(70));
        System.out.println("");

        String groupId = "combined-strategy-" + System.currentTimeMillis();
        Properties props = createConsumerProperties(groupId);

        KafkaConsumer<String, Transaction> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        System.out.println("  Strategy: commitAsync() for throughput, commitSync() on shutdown");
        System.out.println("");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │ NORMAL OPERATION:                                               │");
        System.out.println("  │   poll() → process → commitAsync() → poll() → ...              │");
        System.out.println("  │   (fast, non-blocking, may lose last few on crash)             │");
        System.out.println("  │                                                                 │");
        System.out.println("  │ GRACEFUL SHUTDOWN:                                              │");
        System.out.println("  │   commitSync() → close()                                        │");
        System.out.println("  │   (ensures all processed records are committed)                 │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("");

        int pollCount = 0;

        try {
            while (pollCount < 3) {
                ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(1000));
                pollCount++;

                if (records.isEmpty()) continue;

                System.out.printf("  Poll #%d: Processing %d records...%n", pollCount, records.count());

                for (ConsumerRecord<String, Transaction> record : records) {
                    processRecord(record);
                }

                // Use async for throughput
                consumer.commitAsync((offsets, exception) -> {
                    if (exception != null) {
                        // Log but don't retry - next commit will include these
                        System.err.printf("  [ASYNC] Commit failed (will retry): %s%n",
                                exception.getMessage());
                    }
                });

                System.out.printf("  Poll #%d: commitAsync() sent%n%n", pollCount);
            }

        } finally {
            // CRITICAL: Sync commit before close
            System.out.println("  Graceful shutdown: final commitSync()...");
            try {
                consumer.commitSync(Duration.ofSeconds(5));
                System.out.println("  Final commit successful.");
            } catch (Exception e) {
                System.err.println("  Final commit failed: " + e.getMessage());
            }
            consumer.close();
            System.out.println("  Consumer closed.\n");
        }
    }

    private static void processRecord(ConsumerRecord<String, Transaction> record) {
        // Simulate processing
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static String formatOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (offsets == null || offsets.isEmpty()) return "{}";

        StringBuilder sb = new StringBuilder("{");
        offsets.forEach((tp, om) ->
                sb.append(String.format("P%d=%d, ", tp.partition(), om.offset())));
        sb.setLength(sb.length() - 2);  // Remove last ", "
        sb.append("}");
        return sb.toString();
    }

    private static Properties createConsumerProperties(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        props.put("json.deserializer.type", Transaction.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // CRITICAL: Disable auto-commit for manual control
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return props;
    }

    private static void printAnalysis() {
        System.out.println("=".repeat(70));
        System.out.println("MANUAL OFFSET COMMIT ANALYSIS");
        System.out.println("=".repeat(70));

        System.out.println("\nCOMMIT METHODS:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────────┐");
        System.out.println("  │ commitSync()                                                        │");
        System.out.println("  ├─────────────────────────────────────────────────────────────────────┤");
        System.out.println("  │ • Blocks until broker confirms                                      │");
        System.out.println("  │ • Retries automatically on recoverable errors                       │");
        System.out.println("  │ • Throws exception on unrecoverable errors                          │");
        System.out.println("  │ • Use for: Critical processing, shutdown                            │");
        System.out.println("  └─────────────────────────────────────────────────────────────────────┘");
        System.out.println("");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────────┐");
        System.out.println("  │ commitAsync()                                                       │");
        System.out.println("  ├─────────────────────────────────────────────────────────────────────┤");
        System.out.println("  │ • Returns immediately                                               │");
        System.out.println("  │ • No retry (could cause out-of-order commits)                       │");
        System.out.println("  │ • Optional callback for monitoring                                  │");
        System.out.println("  │ • Use for: High throughput, non-critical                            │");
        System.out.println("  └─────────────────────────────────────────────────────────────────────┘");
        System.out.println("");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────────┐");
        System.out.println("  │ commitSync(Map<TopicPartition, OffsetAndMetadata>)                  │");
        System.out.println("  ├─────────────────────────────────────────────────────────────────────┤");
        System.out.println("  │ • Commits specific offsets for specific partitions                  │");
        System.out.println("  │ • Offset value = NEXT offset to read (current + 1)                  │");
        System.out.println("  │ • Fine-grained control                                              │");
        System.out.println("  │ • Use for: Per-record commit, partial batch commit                  │");
        System.out.println("  └─────────────────────────────────────────────────────────────────────┘");
        System.out.println("");

        System.out.println("\nCOMMIT STRATEGIES COMPARISON:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ┌────────────────────┬──────────────┬──────────────┬──────────────────┐");
        System.out.println("  │ Strategy           │ Throughput   │ Reprocess on │ Use Case         │");
        System.out.println("  │                    │              │ Crash        │                  │");
        System.out.println("  ├────────────────────┼──────────────┼──────────────┼──────────────────┤");
        System.out.println("  │ After each batch   │ High         │ Entire batch │ General purpose  │");
        System.out.println("  ├────────────────────┼──────────────┼──────────────┼──────────────────┤");
        System.out.println("  │ After each record  │ Low          │ 0-1 records  │ Critical txns    │");
        System.out.println("  ├────────────────────┼──────────────┼──────────────┼──────────────────┤");
        System.out.println("  │ Every N records    │ Medium       │ 0 to N-1     │ Balanced         │");
        System.out.println("  ├────────────────────┼──────────────┼──────────────┼──────────────────┤");
        System.out.println("  │ Async + Sync close │ Highest      │ Last batch   │ High throughput  │");
        System.out.println("  └────────────────────┴──────────────┴──────────────┴──────────────────┘");
        System.out.println("");

        System.out.println("\nOFFSET VALUE GOTCHA:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  When committing specific offsets, commit the NEXT offset to read:");
        System.out.println("");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │                                                                 │");
        System.out.println("  │  Record offset = 5                                              │");
        System.out.println("  │                                                                 │");
        System.out.println("  │  WRONG: commit offset 5  → On restart, skips record 5!         │");
        System.out.println("  │  RIGHT: commit offset 6  → On restart, reads from 6            │");
        System.out.println("  │                                                                 │");
        System.out.println("  │  Code: new OffsetAndMetadata(record.offset() + 1)               │");
        System.out.println("  │                                                                 │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("");

        System.out.println("\nWHY commitAsync DOESN'T RETRY:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  Problem: Retry could cause out-of-order commits");
        System.out.println("");
        System.out.println("  Timeline:");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │ T=0: commitAsync(offset=100) → FAILS (network)                  │");
        System.out.println("  │ T=1: commitAsync(offset=200) → SUCCESS                          │");
        System.out.println("  │ T=2: retry commitAsync(offset=100) → SUCCESS                    │");
        System.out.println("  │                                                                 │");
        System.out.println("  │ Result: Committed offset is 100, not 200!                       │");
        System.out.println("  │         Records 101-200 will be reprocessed!                    │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("");
        System.out.println("  Solution: Don't retry async commits. Next successful commit");
        System.out.println("            will include all previous offsets.");
        System.out.println("");

        System.out.println("\nBANKING BEST PRACTICE:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  For fund transfers, use commit-per-record with idempotency:");
        System.out.println("");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │ props.put(\"enable.auto.commit\", \"false\");                       │");
        System.out.println("  │                                                                 │");
        System.out.println("  │ for (ConsumerRecord record : records) {                         │");
        System.out.println("  │     String txnId = record.value().getTransactionId();           │");
        System.out.println("  │                                                                 │");
        System.out.println("  │     // 1. Check if already processed (idempotency)              │");
        System.out.println("  │     if (transactionRepository.exists(txnId)) {                  │");
        System.out.println("  │         continue;  // Skip duplicate                            │");
        System.out.println("  │     }                                                           │");
        System.out.println("  │                                                                 │");
        System.out.println("  │     // 2. Process in database transaction                       │");
        System.out.println("  │     try {                                                       │");
        System.out.println("  │         beginTransaction();                                     │");
        System.out.println("  │         processTransfer(record);                                │");
        System.out.println("  │         transactionRepository.save(txnId);  // Mark processed  │");
        System.out.println("  │         commitTransaction();                                    │");
        System.out.println("  │     } catch (Exception e) {                                     │");
        System.out.println("  │         rollbackTransaction();                                  │");
        System.out.println("  │         throw e;  // Don't commit Kafka offset                  │");
        System.out.println("  │     }                                                           │");
        System.out.println("  │                                                                 │");
        System.out.println("  │     // 3. Commit Kafka offset after DB commit                   │");
        System.out.println("  │     consumer.commitSync(Map.of(                                 │");
        System.out.println("  │         new TopicPartition(record.topic(), record.partition()), │");
        System.out.println("  │         new OffsetAndMetadata(record.offset() + 1)              │");
        System.out.println("  │     ));                                                         │");
        System.out.println("  │ }                                                               │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("");

        System.out.println("\nCONFIG COMPARISON:");
        System.out.println("-".repeat(70));
        System.out.println("| Strategy             | Throughput | Safety   | Complexity |");
        System.out.println("|----------------------|------------|----------|------------|");
        System.out.println("| Auto-commit          | Highest    | Lowest   | Simple     |");
        System.out.println("| commitSync (batch)   | Medium     | High     | Simple     |");
        System.out.println("| commitSync (record)  | Low        | Highest  | Medium     |");
        System.out.println("| commitAsync + Sync   | High       | Medium   | Medium     |");
        System.out.println("| Commit every N       | Med-High   | Med-High | Medium     |");
        System.out.println("-".repeat(70));
    }
}