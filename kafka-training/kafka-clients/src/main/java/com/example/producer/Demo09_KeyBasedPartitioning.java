package com.example.producer;

import com.example.Transaction;
import com.example.JsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * DEMO 09: Key-based Partitioning
 *
 * WHAT CHANGED FROM DEMO 08:
 * +---------------------------+-------------------+---------------------+
 * | Aspect                    | Demo 08           | Demo 09             |
 * +---------------------------+-------------------+---------------------+
 * | Key usage                 | Random/varied     | Consistent by acc   |
 * | Partition selection       | Varies            | Deterministic       |
 * | Message ordering          | No guarantee      | Per-key ordering    |
 * +---------------------------+-------------------+---------------------+
 *
 * WHY KEYS MATTER:
 *
 * Without Key (key=null):
 * - Round-robin or sticky partition assignment
 * - Messages scattered across partitions
 * - NO ordering guarantee
 *
 * With Key:
 * - Same key → Same partition (always)
 * - Ordering guaranteed within partition
 * - Critical for event sequences
 *
 * PARTITIONING FORMULA:
 * partition = murmur2(key) % num_partitions
 *
 * BANKING EXAMPLE:
 *
 * Account ACC001 transactions MUST be ordered:
 * 1. Open Account
 * 2. Deposit ₹10,000
 * 3. Withdraw ₹5,000
 * 4. Close Account
 *
 * If out of order → Withdraw before deposit → FAILURE!
 *
 * Solution: Use account_id as key
 * All ACC001 events → Same partition → Ordered processing
 *
 * Banking Scenario: Account lifecycle, transaction sequences, audit trails
 */
public class Demo09_KeyBasedPartitioning {

    public static void main(String[] args) throws InterruptedException {

        System.out.println("=".repeat(70));
        System.out.println("DEMO 09: Key-based Partitioning");
        System.out.println("=".repeat(70));
        System.out.println("Demonstrating message ordering with keys\n");

        // Part 1: Without keys (no ordering)
        runWithoutKeys();

        // Part 2: With keys (guaranteed ordering per account)
        runWithKeys();

        printAnalysis();
    }

    private static void runWithoutKeys() throws InterruptedException {
        Properties props = createBaseProperties();
        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(props);

        String topic = "fund-transfer-events";

        System.out.println("-".repeat(70));
        System.out.println("PART 1: Without Keys (key=null)");
        System.out.println("-".repeat(70));
        System.out.println("Sending 10 transactions for ACC001 without key...\n");

        Map<Integer, Integer> partitionCount = new HashMap<>();
        CountDownLatch latch = new CountDownLatch(10);

        for (int i = 1; i <= 10; i++) {
            final int seq = i;
            Transaction txn = Transaction.builder()
                    .transactionId("TXN-" + i)
                    .fromAccount("ACC001")
                    .toAccount("ACC002")
                    .amount(new BigDecimal(i * 1000))
                    .type("SEQ-" + i)
                    .build();

            // NO KEY - will be distributed across partitions
            ProducerRecord<String, Transaction> record =
                    new ProducerRecord<>(topic, null, txn);

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    partitionCount.merge(metadata.partition(), 1, Integer::sum);
                    System.out.printf("  SEQ-%d → Partition: %d, Offset: %d%n",
                            seq, metadata.partition(), metadata.offset());
                }
                latch.countDown();
            });

            // Small delay to see round-robin effect
            Thread.sleep(10);
        }

        latch.await();
        producer.close();

        System.out.println("\n  Partition Distribution: " + partitionCount);
        System.out.println("  ⚠ Messages scattered! Order NOT guaranteed!\n");
    }

    private static void runWithKeys() throws InterruptedException {
        Properties props = createBaseProperties();
        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(props);

        String topic = "fund-transfer-events";

        System.out.println("-".repeat(70));
        System.out.println("PART 2: With Keys (key=account_id)");
        System.out.println("-".repeat(70));
        System.out.println("Sending transactions for 3 accounts with account_id as key...\n");

        String[] accounts = {"ACC001", "ACC002", "ACC003"};
        Map<String, Integer> accountPartitions = new HashMap<>();
        CountDownLatch latch = new CountDownLatch(15);

        // Send 5 transactions per account
        for (String account : accounts) {
            System.out.printf("  Account %s transactions:%n", account);

            for (int i = 1; i <= 5; i++) {
                final int seq = i;
                final String acc = account;

                Transaction txn = Transaction.builder()
                        .transactionId(account + "-TXN-" + i)
                        .fromAccount(account)
                        .toAccount("MERCHANT")
                        .amount(new BigDecimal(i * 500))
                        .type(getTransactionType(i))
                        .build();

                // KEY = account_id → All account txns go to same partition
                ProducerRecord<String, Transaction> record =
                        new ProducerRecord<>(topic, account, txn);

                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        accountPartitions.put(acc, metadata.partition());
                        System.out.printf("    %d. %s → Partition: %d, Offset: %d%n",
                                seq, getTransactionType(seq), metadata.partition(), metadata.offset());
                    }
                    latch.countDown();
                });

                Thread.sleep(5);
            }
            System.out.println();
        }

        latch.await();
        producer.close();

        System.out.println("  Account → Partition Mapping:");
        accountPartitions.forEach((acc, part) ->
                System.out.printf("    %s → Partition %d%n", acc, part));
        System.out.println("\n  ✓ All transactions per account in SAME partition!");
        System.out.println("  ✓ Order GUARANTEED within each account!\n");
    }

    private static String getTransactionType(int seq) {
        return switch (seq) {
            case 1 -> "ACCOUNT_OPEN";
            case 2 -> "DEPOSIT";
            case 3 -> "WITHDRAWAL";
            case 4 -> "TRANSFER";
            case 5 -> "STATEMENT";
            default -> "OTHER";
        };
    }

    private static Properties createBaseProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        return props;
    }

    private static void printAnalysis() {
        System.out.println("=".repeat(70));
        System.out.println("KEY-BASED PARTITIONING ANALYSIS");
        System.out.println("=".repeat(70));

        System.out.println("\nPARTITIONING STRATEGIES:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  1. No Key (key=null):");
        System.out.println("  ┌─────────────────────────────────────────────────────┐");
        System.out.println("  │ Kafka 2.4+: Sticky Partitioner                      │");
        System.out.println("  │ - Stick to one partition until batch full           │");
        System.out.println("  │ - Then switch to next partition                     │");
        System.out.println("  │ - Better batching, but no ordering guarantee        │");
        System.out.println("  └─────────────────────────────────────────────────────┘");
        System.out.println("");
        System.out.println("  2. With Key:");
        System.out.println("  ┌─────────────────────────────────────────────────────┐");
        System.out.println("  │ partition = murmur2(key) % num_partitions           │");
        System.out.println("  │ - Deterministic: same key → same partition          │");
        System.out.println("  │ - Ordering guaranteed per key                       │");
        System.out.println("  └─────────────────────────────────────────────────────┘");
        System.out.println("");

        System.out.println("\nORDERING GUARANTEE:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  Without Key:");
        System.out.println("  ACC001 txns scattered:");
        System.out.println("  ┌────────────┐ ┌────────────┐ ┌────────────┐");
        System.out.println("  │ Partition 0│ │ Partition 1│ │ Partition 2│");
        System.out.println("  │            │ │            │ │            │");
        System.out.println("  │ TXN-1      │ │ TXN-2      │ │ TXN-3      │");
        System.out.println("  │ TXN-4      │ │ TXN-5      │ │            │");
        System.out.println("  └────────────┘ └────────────┘ └────────────┘");
        System.out.println("  Consumer may process: TXN-2, TXN-1, TXN-5... (WRONG!)");
        System.out.println("");
        System.out.println("  With Key (key=ACC001):");
        System.out.println("  ACC001 txns in one partition:");
        System.out.println("  ┌────────────┐ ┌────────────┐ ┌────────────┐");
        System.out.println("  │ Partition 0│ │ Partition 1│ │ Partition 2│");
        System.out.println("  │            │ │            │ │            │");
        System.out.println("  │ TXN-1      │ │ (other     │ │ (other     │");
        System.out.println("  │ TXN-2      │ │  accounts) │ │  accounts) │");
        System.out.println("  │ TXN-3      │ │            │ │            │");
        System.out.println("  │ TXN-4      │ │            │ │            │");
        System.out.println("  │ TXN-5      │ │            │ │            │");
        System.out.println("  └────────────┘ └────────────┘ └────────────┘");
        System.out.println("  Consumer processes: TXN-1, TXN-2, TXN-3... (CORRECT!)");
        System.out.println("");

        System.out.println("\nBANKING KEY STRATEGIES:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ┌───────────────────┬──────────────────┬─────────────────────┐");
        System.out.println("  │ Event Type        │ Key              │ Reason              │");
        System.out.println("  ├───────────────────┼──────────────────┼─────────────────────┤");
        System.out.println("  │ Account events    │ account_id       │ Lifecycle ordering  │");
        System.out.println("  │ Transactions      │ from_account_id  │ Debit sequence      │");
        System.out.println("  │ Customer events   │ customer_id      │ Profile consistency │");
        System.out.println("  │ Card events       │ card_number      │ Card lifecycle      │");
        System.out.println("  │ Loan events       │ loan_id          │ Loan processing     │");
        System.out.println("  │ Branch events     │ branch_code      │ Branch aggregation  │");
        System.out.println("  └───────────────────┴──────────────────┴─────────────────────┘");
        System.out.println("");

        System.out.println("\nHOT PARTITION PROBLEM:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  Problem: One key has massive traffic");
        System.out.println("");
        System.out.println("  Example: Corporate account with 100K txns/day");
        System.out.println("  ┌────────────┐ ┌────────────┐ ┌────────────┐");
        System.out.println("  │ Partition 0│ │ Partition 1│ │ Partition 2│");
        System.out.println("  │ 100,000    │ │ 1,000      │ │ 1,000      │");
        System.out.println("  │ messages   │ │ messages   │ │ messages   │");
        System.out.println("  │ (HOT!)     │ │            │ │            │");
        System.out.println("  └────────────┘ └────────────┘ └────────────┘");
        System.out.println("");
        System.out.println("  Solutions:");
        System.out.println("  1. Compound key: account_id + sub_category");
        System.out.println("  2. Add salt: account_id + (txn_id % N)");
        System.out.println("  3. Custom partitioner (Demo 10)");
        System.out.println("");

        System.out.println("\nKEY BEST PRACTICES:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ✓ DO:");
        System.out.println("    • Use natural business key (account_id, customer_id)");
        System.out.println("    • Keep keys consistent across related events");
        System.out.println("    • Use string keys for portability");
        System.out.println("    • Monitor partition distribution");
        System.out.println("");
        System.out.println("  ✗ DON'T:");
        System.out.println("    • Use timestamp as key (poor distribution)");
        System.out.println("    • Use random UUID if ordering needed");
        System.out.println("    • Change key format (breaks partition assignment)");
        System.out.println("    • Use very long keys (overhead)");
        System.out.println("");

        System.out.println("\nPARTITION COUNT WARNING:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ⚠ If partition count changes, key→partition mapping changes!");
        System.out.println("");
        System.out.println("  Before (3 partitions):");
        System.out.println("    ACC001 → murmur2(ACC001) % 3 = Partition 1");
        System.out.println("");
        System.out.println("  After (6 partitions):");
        System.out.println("    ACC001 → murmur2(ACC001) % 6 = Partition 4");
        System.out.println("");
        System.out.println("  Result: New messages go to different partition!");
        System.out.println("  Solution: Plan partition count upfront or use custom partitioner");
        System.out.println("");

        System.out.println("\nCONFIG COMPARISON:");
        System.out.println("-".repeat(70));
        System.out.println("| Aspect              | Demo08         | Demo09            |");
        System.out.println("|---------------------|----------------|-------------------|");
        System.out.println("| Key                 | Varies         | account_id        |");
        System.out.println("| Partition selection | Sticky/random  | Deterministic     |");
        System.out.println("| Ordering            | None           | Per-account       |");
        System.out.println("| Use case            | High throughput| Event sequences   |");
        System.out.println("-".repeat(70));
    }
}