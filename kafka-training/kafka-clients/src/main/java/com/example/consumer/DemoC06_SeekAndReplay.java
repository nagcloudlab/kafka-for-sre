package com.example.consumer;

import com.example.Transaction;
import com.example.JsonDeserializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

/**
 * DEMO C06: Seek & Replay
 *
 * SEEK OPERATIONS:
 * - seekToBeginning(): Replay from start
 * - seekToEnd(): Skip to latest
 * - seek(partition, offset): Specific offset
 * - offsetsForTimes(): Seek to timestamp
 *
 * USE CASES:
 * 1. Replay messages after bug fix
 * 2. Reprocess data with new logic
 * 3. Recovery from processing errors
 * 4. Time-based replay (last 24 hours)
 * 5. Reset consumer group position
 */
public class DemoC06_SeekAndReplay {

    private static final String TOPIC = "fund-transfer-events";

    public static void main(String[] args) {
        System.out.println("=".repeat(70));
        System.out.println("DEMO C06: Seek & Replay");
        System.out.println("=".repeat(70));

        demonstrateSeekToBeginning();
        demonstrateSeekToEnd();
        demonstrateSeekToSpecificOffset();
        demonstrateSeekToTimestamp();
        printAnalysis();
    }

    private static void demonstrateSeekToBeginning() {
        System.out.println("\n" + "-".repeat(70));
        System.out.println("PART 1: seekToBeginning() - Replay from Start");
        System.out.println("-".repeat(70));

        Properties props = createConsumerProperties("seek-beginning-" + System.currentTimeMillis());
        KafkaConsumer<String, Transaction> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(TOPIC));
        consumer.poll(Duration.ofMillis(100)); // Trigger assignment

        Set<TopicPartition> assignment = consumer.assignment();
        System.out.printf("\nAssigned partitions: %d%n", assignment.size());

        // Show current positions
        System.out.println("\nBEFORE seekToBeginning():");
        for (TopicPartition tp : assignment) {
            System.out.printf("  P%d: position=%d, beginning=%d, end=%d%n",
                    tp.partition(),
                    consumer.position(tp),
                    consumer.beginningOffsets(Collections.singleton(tp)).get(tp),
                    consumer.endOffsets(Collections.singleton(tp)).get(tp));
        }

        // Seek to beginning
        System.out.println("\nExecuting: seekToBeginning()");
        consumer.seekToBeginning(assignment);

        System.out.println("\nAFTER seekToBeginning():");
        for (TopicPartition tp : assignment) {
            System.out.printf("  P%d: position=%d (reset to beginning)%n",
                    tp.partition(), consumer.position(tp));
        }

        // Read first few messages
        System.out.println("\nReading first 5 messages:");
        ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(2000));
        int count = 0;
        for (ConsumerRecord<String, Transaction> record : records) {
            if (count++ < 5) {
                System.out.printf("  P%d-Offset%d: %s → %s, ₹%s%n",
                        record.partition(), record.offset(),
                        record.value().getFromAccount(),
                        record.value().getToAccount(),
                        record.value().getAmount());
            } else break;
        }

        consumer.close();
    }

    private static void demonstrateSeekToEnd() {
        System.out.println("\n" + "-".repeat(70));
        System.out.println("PART 2: seekToEnd() - Skip to Latest");
        System.out.println("-".repeat(70));

        Properties props = createConsumerProperties("seek-end-" + System.currentTimeMillis());
        KafkaConsumer<String, Transaction> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(TOPIC));
        consumer.poll(Duration.ofMillis(100));

        Set<TopicPartition> assignment = consumer.assignment();

        System.out.println("\nBEFORE seekToEnd():");
        for (TopicPartition tp : assignment) {
            System.out.printf("  P%d: position=%d%n", tp.partition(), consumer.position(tp));
        }

        System.out.println("\nExecuting: seekToEnd()");
        consumer.seekToEnd(assignment);

        System.out.println("\nAFTER seekToEnd():");
        for (TopicPartition tp : assignment) {
            System.out.printf("  P%d: position=%d (at end)%n", tp.partition(), consumer.position(tp));
        }

        System.out.println("\nPolling (should receive 0 old messages, only new ones):");
        ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(2000));
        System.out.printf("  Received: %d messages%n", records.count());

        consumer.close();
    }

    private static void demonstrateSeekToSpecificOffset() {
        System.out.println("\n" + "-".repeat(70));
        System.out.println("PART 3: seek() - Specific Offset");
        System.out.println("-".repeat(70));

        Properties props = createConsumerProperties("seek-offset-" + System.currentTimeMillis());
        KafkaConsumer<String, Transaction> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(TOPIC));
        consumer.poll(Duration.ofMillis(100));

        Set<TopicPartition> assignment = consumer.assignment();
        TopicPartition firstPartition = assignment.iterator().next();

        long endOffset = consumer.endOffsets(Collections.singleton(firstPartition)).get(firstPartition);
        long targetOffset = Math.max(0, endOffset - 10); // Last 10 messages

        System.out.printf("\nPartition P%d:%n", firstPartition.partition());
        System.out.printf("  End offset: %d%n", endOffset);
        System.out.printf("  Target offset: %d (last 10 messages)%n", targetOffset);

        System.out.printf("\nExecuting: seek(P%d, %d)%n", firstPartition.partition(), targetOffset);
        consumer.seek(firstPartition, targetOffset);

        System.out.printf("\nPosition after seek: %d%n", consumer.position(firstPartition));

        System.out.println("\nReading messages from offset " + targetOffset + ":");
        ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(2000));
        for (ConsumerRecord<String, Transaction> record : records) {
            if (record.partition() == firstPartition.partition()) {
                System.out.printf("  Offset %d: %s%n", record.offset(), record.key());
            }
        }

        consumer.close();
    }

    private static void demonstrateSeekToTimestamp() {
        System.out.println("\n" + "-".repeat(70));
        System.out.println("PART 4: offsetsForTimes() - Seek to Timestamp");
        System.out.println("-".repeat(70));

        Properties props = createConsumerProperties("seek-timestamp-" + System.currentTimeMillis());
        KafkaConsumer<String, Transaction> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(TOPIC));
        consumer.poll(Duration.ofMillis(100));

        Set<TopicPartition> assignment = consumer.assignment();

        // Seek to 5 minutes ago
        long fiveMinutesAgo = System.currentTimeMillis() - (5 * 60 * 1000);

        System.out.printf("\nTarget timestamp: %s%n",
                LocalDateTime.ofInstant(Instant.ofEpochMilli(fiveMinutesAgo), ZoneId.systemDefault()));

        // Build timestamp map
        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        for (TopicPartition tp : assignment) {
            timestampsToSearch.put(tp, fiveMinutesAgo);
        }

        System.out.println("\nExecuting: offsetsForTimes()");
        Map<TopicPartition, OffsetAndTimestamp> offsetMap = consumer.offsetsForTimes(timestampsToSearch);

        System.out.println("\nResults per partition:");
        for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetMap.entrySet()) {
            TopicPartition tp = entry.getKey();
            OffsetAndTimestamp offsetAndTimestamp = entry.getValue();

            if (offsetAndTimestamp != null) {
                System.out.printf("  P%d: offset=%d, timestamp=%s%n",
                        tp.partition(),
                        offsetAndTimestamp.offset(),
                        LocalDateTime.ofInstant(
                                Instant.ofEpochMilli(offsetAndTimestamp.timestamp()),
                                ZoneId.systemDefault()));

                // Seek to this offset
                consumer.seek(tp, offsetAndTimestamp.offset());
            } else {
                System.out.printf("  P%d: No messages found for timestamp%n", tp.partition());
            }
        }

        System.out.println("\nReading messages from 5 minutes ago:");
        ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(2000));
        System.out.printf("  Received: %d messages%n", records.count());

        int count = 0;
        for (ConsumerRecord<String, Transaction> record : records) {
            if (count++ < 3) {
                System.out.printf("  P%d-Offset%d: timestamp=%s%n",
                        record.partition(), record.offset(),
                        LocalDateTime.ofInstant(
                                Instant.ofEpochMilli(record.timestamp()),
                                ZoneId.systemDefault()));
            }
        }

        consumer.close();
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
        System.out.println("SEEK & REPLAY ANALYSIS");
        System.out.println("=".repeat(70));

        System.out.println("\nSEEK METHODS:");
        System.out.println("-".repeat(70));
        System.out.println("┌────────────────────────────┬────────────────────────────────────────┐");
        System.out.println("│ Method                     │ Use Case                               │");
        System.out.println("├────────────────────────────┼────────────────────────────────────────┤");
        System.out.println("│ seekToBeginning()          │ Replay all messages from start         │");
        System.out.println("├────────────────────────────┼────────────────────────────────────────┤");
        System.out.println("│ seekToEnd()                │ Skip to latest, ignore old messages    │");
        System.out.println("├────────────────────────────┼────────────────────────────────────────┤");
        System.out.println("│ seek(tp, offset)           │ Start from specific offset             │");
        System.out.println("├────────────────────────────┼────────────────────────────────────────┤");
        System.out.println("│ offsetsForTimes()          │ Find offset for timestamp, then seek   │");
        System.out.println("└────────────────────────────┴────────────────────────────────────────┘");

        System.out.println("\n\nBANKING USE CASES:");
        System.out.println("-".repeat(70));
        System.out.println("1. BUG FIX REPLAY:");
        System.out.println("   - Bug found in payment processing");
        System.out.println("   - Fix deployed");
        System.out.println("   - seekToBeginning() to reprocess all transactions");
        System.out.println("   - Use idempotency keys to avoid duplicates");
        System.out.println("");
        System.out.println("2. SPECIFIC DATE REPLAY:");
        System.out.println("   - Reprocess transactions from 2025-01-15");
        System.out.println("   - Use offsetsForTimes() to find start offset");
        System.out.println("   - Process until end of day");
        System.out.println("");
        System.out.println("3. DISASTER RECOVERY:");
        System.out.println("   - Database crashed, restored from backup");
        System.out.println("   - Backup timestamp: 2025-01-20 10:00:00");
        System.out.println("   - Use offsetsForTimes() to replay from backup point");
        System.out.println("");

        System.out.println("\nCLI ALTERNATIVE (Reset Consumer Group):");
        System.out.println("-".repeat(70));
        System.out.println("# Reset to earliest");
        System.out.println("kafka-consumer-groups.sh --bootstrap-server localhost:9092 \\");
        System.out.println("  --group payment-processors \\");
        System.out.println("  --topic fund-transfer-events \\");
        System.out.println("  --reset-offsets --to-earliest --execute");
        System.out.println("");
        System.out.println("# Reset to specific offset");
        System.out.println("kafka-consumer-groups.sh --bootstrap-server localhost:9092 \\");
        System.out.println("  --group payment-processors \\");
        System.out.println("  --topic fund-transfer-events:0 \\");
        System.out.println("  --reset-offsets --to-offset 1000 --execute");
        System.out.println("");
        System.out.println("# Reset to timestamp");
        System.out.println("kafka-consumer-groups.sh --bootstrap-server localhost:9092 \\");
        System.out.println("  --group payment-processors \\");
        System.out.println("  --topic fund-transfer-events \\");
        System.out.println("  --reset-offsets --to-datetime 2025-01-20T10:00:00.000 --execute");
        System.out.println("");

        System.out.println("\nCAUTIONS:");
        System.out.println("-".repeat(70));
        System.out.println("⚠ IMPORTANT: Ensure idempotent processing when replaying!");
        System.out.println("⚠ Stop all consumers before resetting offsets via CLI");
        System.out.println("⚠ Seek only works on ASSIGNED partitions (after poll)");
        System.out.println("⚠ offsetsForTimes returns null if no message at timestamp");
        System.out.println("⚠ Replaying can cause duplicate processing without idempotency");
        System.out.println("-".repeat(70));
    }
}