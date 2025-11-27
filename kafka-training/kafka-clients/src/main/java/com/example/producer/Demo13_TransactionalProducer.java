package com.example.producer;

import com.example.Transaction;
import com.example.JsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.math.BigDecimal;
import java.util.Properties;
import java.util.UUID;

/**
 * DEMO 13: Transactional Producer
 *
 * WHAT CHANGED FROM DEMO 12:
 * +---------------------------+-------------------+---------------------+
 * | Aspect                    | Demo 12           | Demo 13             |
 * +---------------------------+-------------------+---------------------+
 * | Delivery guarantee        | At-least-once     | Exactly-once        |
 * | Scope                     | Single partition  | Multi-partition     |
 * | Atomicity                 | Per message       | Batch of messages   |
 * | transactional.id          | None              | Required            |
 * +---------------------------+-------------------+---------------------+
 *
 * IDEMPOTENT vs TRANSACTIONAL:
 *
 * IDEMPOTENT (Demo 06):
 * ┌─────────────────────────────────────────────────────────────────┐
 * │ Exactly-once per PARTITION                                      │
 * │ - Prevents duplicates from retries                              │
 * │ - Single partition scope                                        │
 * │ - No atomicity across messages                                  │
 * └─────────────────────────────────────────────────────────────────┘
 *
 * TRANSACTIONAL (Demo 13):
 * ┌─────────────────────────────────────────────────────────────────┐
 * │ Exactly-once across MULTIPLE PARTITIONS/TOPICS                  │
 * │ - All messages in transaction commit together                   │
 * │ - Or all abort together (atomic)                                │
 * │ - Cross-partition, cross-topic atomicity                        │
 * └─────────────────────────────────────────────────────────────────┘
 *
 * TRANSACTION FLOW:
 *
 * beginTransaction()
 *     │
 *     ├── send(topic1, partition0, msg1)  ──┐
 *     ├── send(topic1, partition1, msg2)  ──┼── All buffered
 *     ├── send(topic2, partition0, msg3)  ──┘
 *     │
 *     ▼
 * commitTransaction()  ──► All messages visible atomically
 *        OR
 * abortTransaction()   ──► All messages discarded
 *
 * BANKING SCENARIO:
 * Fund transfer requires atomic writes to:
 * 1. debit-events topic (debit from sender)
 * 2. credit-events topic (credit to receiver)
 * 3. audit-events topic (transaction log)
 *
 * Either ALL succeed or NONE - no partial transfers!
 */
public class Demo13_TransactionalProducer {

    public static void main(String[] args) {

        System.out.println("=".repeat(70));
        System.out.println("DEMO 13: Transactional Producer");
        System.out.println("=".repeat(70));
        System.out.println("Exactly-once semantics with atomic multi-topic writes\n");

        // Part 1: Basic transaction
        runBasicTransactionDemo();

        // Part 2: Transaction with rollback
        runTransactionRollbackDemo();

        // Part 3: Banking fund transfer (atomic)
        runBankingTransferDemo();

        printAnalysis();
    }

    private static void runBasicTransactionDemo() {
        System.out.println("-".repeat(70));
        System.out.println("PART 1: Basic Transaction (Commit)");
        System.out.println("-".repeat(70));
        System.out.println("");

        Properties props = createTransactionalProperties("txn-demo-basic-" + System.currentTimeMillis());
        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(props);

        // REQUIRED: Initialize transactions before use
        producer.initTransactions();
        System.out.println("  [INIT] Transactions initialized\n");

        String topic = "fund-transfer-events";

        try {
            // BEGIN TRANSACTION
            producer.beginTransaction();
            System.out.println("  [BEGIN] Transaction started");

            // Send multiple messages (all part of same transaction)
            for (int i = 1; i <= 3; i++) {
                Transaction txn = Transaction.builder()
                        .fromAccount("ACC00" + i)
                        .toAccount("ACC00" + (i + 1))
                        .amount(new BigDecimal(i * 10000))
                        .type("TXN_BATCH")
                        .build();

                ProducerRecord<String, Transaction> record =
                        new ProducerRecord<>(topic, txn.getFromAccount(), txn);

                producer.send(record);
                System.out.printf("  [SEND] Message %d queued (not visible yet)%n", i);
            }

            // COMMIT - All messages become visible atomically
            producer.commitTransaction();
            System.out.println("  [COMMIT] Transaction committed - all messages visible!\n");

        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // Fatal errors - cannot recover
            System.err.println("  [FATAL] " + e.getMessage());
            producer.close();
        } catch (KafkaException e) {
            // Abort transaction on error
            System.err.println("  [ERROR] " + e.getMessage());
            producer.abortTransaction();
        }

        producer.close();
    }

    private static void runTransactionRollbackDemo() {
        System.out.println("-".repeat(70));
        System.out.println("PART 2: Transaction Rollback (Abort)");
        System.out.println("-".repeat(70));
        System.out.println("");

        Properties props = createTransactionalProperties("txn-demo-rollback-" + System.currentTimeMillis());
        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(props);

        producer.initTransactions();
        System.out.println("  [INIT] Transactions initialized\n");

        String topic = "fund-transfer-events";

        try {
            producer.beginTransaction();
            System.out.println("  [BEGIN] Transaction started");

            // Send some messages
            for (int i = 1; i <= 3; i++) {
                Transaction txn = Transaction.builder()
                        .fromAccount("ROLLBACK-ACC00" + i)
                        .toAccount("ROLLBACK-ACC00" + (i + 1))
                        .amount(new BigDecimal(i * 5000))
                        .type("WILL_ROLLBACK")
                        .build();

                producer.send(new ProducerRecord<>(topic, txn.getFromAccount(), txn));
                System.out.printf("  [SEND] Message %d queued%n", i);
            }

            // Simulate business validation failure
            boolean validationFailed = true;
            if (validationFailed) {
                throw new RuntimeException("Simulated: Insufficient balance");
            }

            producer.commitTransaction();

        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            System.err.println("  [FATAL] " + e.getMessage());
        } catch (Exception e) {
            System.out.println("  [ERROR] " + e.getMessage());
            System.out.println("  [ABORT] Rolling back transaction...");
            producer.abortTransaction();
            System.out.println("  [ABORT] Transaction aborted - NO messages written!\n");
        }

        producer.close();
    }

    private static void runBankingTransferDemo() {
        System.out.println("-".repeat(70));
        System.out.println("PART 3: Banking Fund Transfer (Atomic Multi-Topic)");
        System.out.println("-".repeat(70));
        System.out.println("");

        Properties props = createTransactionalProperties("txn-banking-" + System.currentTimeMillis());
        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(props);

        producer.initTransactions();
        System.out.println("  [INIT] Transactions initialized");
        System.out.println("");

        // Banking scenario: Transfer ₹1,00,000 from ACC001 to ACC002
        String debitTopic = "debit-events";
        String creditTopic = "credit-events";
        String auditTopic = "audit-events";

        String transferId = "TRF-" + UUID.randomUUID().toString().substring(0, 8);
        BigDecimal amount = new BigDecimal("100000.00");
        String fromAccount = "ACC001";
        String toAccount = "ACC002";

        System.out.printf("  Transfer: %s → %s, Amount: ₹%s%n", fromAccount, toAccount, amount);
        System.out.printf("  Transfer ID: %s%n%n", transferId);

        try {
            producer.beginTransaction();
            System.out.println("  [BEGIN] Transaction started");

            // 1. DEBIT event
            Transaction debit = Transaction.builder()
                    .transactionId(transferId)
                    .fromAccount(fromAccount)
                    .toAccount(toAccount)
                    .amount(amount.negate())  // Negative for debit
                    .type("DEBIT")
                    .status("PENDING")
                    .build();
            producer.send(new ProducerRecord<>(debitTopic, fromAccount, debit));
            System.out.printf("  [SEND] Debit event → %s (₹-%s)%n", debitTopic, amount);

            // 2. CREDIT event
            Transaction credit = Transaction.builder()
                    .transactionId(transferId)
                    .fromAccount(fromAccount)
                    .toAccount(toAccount)
                    .amount(amount)  // Positive for credit
                    .type("CREDIT")
                    .status("PENDING")
                    .build();
            producer.send(new ProducerRecord<>(creditTopic, toAccount, credit));
            System.out.printf("  [SEND] Credit event → %s (₹+%s)%n", creditTopic, amount);

            // 3. AUDIT event
            Transaction audit = Transaction.builder()
                    .transactionId(transferId)
                    .fromAccount(fromAccount)
                    .toAccount(toAccount)
                    .amount(amount)
                    .type("FUND_TRANSFER")
                    .status("COMPLETED")
                    .build();
            producer.send(new ProducerRecord<>(auditTopic, transferId, audit));
            System.out.printf("  [SEND] Audit event → %s%n", auditTopic);

            // Simulate business validation
            boolean sufficientBalance = true;  // In real scenario, check balance

            if (!sufficientBalance) {
                throw new RuntimeException("Insufficient balance");
            }

            // COMMIT - All 3 events become visible atomically
            producer.commitTransaction();
            System.out.println("");
            System.out.println("  [COMMIT] Transaction committed!");
            System.out.println("           All 3 events (debit, credit, audit) visible atomically");
            System.out.println("");

        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            System.err.println("  [FATAL] " + e.getMessage());
        } catch (Exception e) {
            System.out.println("");
            System.out.println("  [ERROR] " + e.getMessage());
            System.out.println("  [ABORT] Rolling back all events...");
            producer.abortTransaction();
            System.out.println("  [ABORT] NO events written - transfer cancelled");
            System.out.println("");
        }

        producer.close();
    }

    private static Properties createTransactionalProperties(String transactionalId) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

        // REQUIRED for transactions
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);

        // Auto-enabled with transactions, but explicit for clarity
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // Recommended for transactions
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        return props;
    }

    private static void printAnalysis() {
        System.out.println("=".repeat(70));
        System.out.println("TRANSACTIONAL PRODUCER ANALYSIS");
        System.out.println("=".repeat(70));

        System.out.println("\nTRANSACTION API:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  // 1. Configure with transactional.id");
        System.out.println("  props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, \"my-txn-id\");");
        System.out.println("");
        System.out.println("  // 2. Initialize (call ONCE at startup)");
        System.out.println("  producer.initTransactions();");
        System.out.println("");
        System.out.println("  // 3. Transaction block");
        System.out.println("  try {");
        System.out.println("      producer.beginTransaction();");
        System.out.println("      producer.send(record1);");
        System.out.println("      producer.send(record2);");
        System.out.println("      producer.send(record3);");
        System.out.println("      producer.commitTransaction();  // All or nothing");
        System.out.println("  } catch (Exception e) {");
        System.out.println("      producer.abortTransaction();   // Rollback");
        System.out.println("  }");
        System.out.println("");

        System.out.println("\nIDEMPOTENT vs TRANSACTIONAL:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ┌────────────────────────────────────────────────────────────────┐");
        System.out.println("  │                    IDEMPOTENT (Demo 06)                        │");
        System.out.println("  ├────────────────────────────────────────────────────────────────┤");
        System.out.println("  │ Scope:     Single partition                                    │");
        System.out.println("  │ Guarantee: No duplicates from retries                          │");
        System.out.println("  │ Atomicity: Per message                                         │");
        System.out.println("  │ Use case:  Simple duplicate prevention                         │");
        System.out.println("  └────────────────────────────────────────────────────────────────┘");
        System.out.println("");
        System.out.println("  ┌────────────────────────────────────────────────────────────────┐");
        System.out.println("  │                  TRANSACTIONAL (Demo 13)                       │");
        System.out.println("  ├────────────────────────────────────────────────────────────────┤");
        System.out.println("  │ Scope:     Multiple partitions, multiple topics                │");
        System.out.println("  │ Guarantee: Exactly-once across entire batch                    │");
        System.out.println("  │ Atomicity: All messages commit or abort together               │");
        System.out.println("  │ Use case:  Distributed transactions, event sourcing            │");
        System.out.println("  └────────────────────────────────────────────────────────────────┘");
        System.out.println("");

        System.out.println("\nTRANSACTION FLOW VISUALIZATION:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  Producer                          Broker                    Consumer");
        System.out.println("  ────────                          ──────                    ────────");
        System.out.println("      │                                │                          │");
        System.out.println("      │ initTransactions()             │                          │");
        System.out.println("      │───────────────────────────────►│                          │");
        System.out.println("      │                                │                          │");
        System.out.println("      │ beginTransaction()             │                          │");
        System.out.println("      │───────────────────────────────►│                          │");
        System.out.println("      │                                │                          │");
        System.out.println("      │ send(msg1) ───────────────────►│ [uncommitted]            │");
        System.out.println("      │ send(msg2) ───────────────────►│ [uncommitted]  ─ ─ ─ ─ ─►│ (not visible)");
        System.out.println("      │ send(msg3) ───────────────────►│ [uncommitted]            │");
        System.out.println("      │                                │                          │");
        System.out.println("      │ commitTransaction()            │                          │");
        System.out.println("      │───────────────────────────────►│ [committed] ────────────►│ (all visible)");
        System.out.println("      │                                │                          │");
        System.out.println("");
        System.out.println("  With isolation.level=read_committed, consumer only sees committed messages");
        System.out.println("");

        System.out.println("\nBANKING FUND TRANSFER - WHY TRANSACTIONS:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  WITHOUT TRANSACTIONS (Partial failure possible):");
        System.out.println("  ────────────────────────────────────────────────");
        System.out.println("    1. send(debit-events, -₹1,00,000)  ✓ Success");
        System.out.println("    2. send(credit-events, +₹1,00,000) ✗ FAILED (network issue)");
        System.out.println("    3. send(audit-events, transfer)    ✗ NOT SENT");
        System.out.println("");
        System.out.println("    Result: Money debited but not credited! INCONSISTENT!");
        System.out.println("");
        System.out.println("  WITH TRANSACTIONS (Atomic):");
        System.out.println("  ─────────────────────────────");
        System.out.println("    beginTransaction()");
        System.out.println("    1. send(debit-events, -₹1,00,000)  [buffered]");
        System.out.println("    2. send(credit-events, +₹1,00,000) [buffered]");
        System.out.println("    3. send(audit-events, transfer)    [buffered]");
        System.out.println("    commitTransaction() OR abortTransaction()");
        System.out.println("");
        System.out.println("    Result: ALL succeed or NONE - Always consistent!");
        System.out.println("");

        System.out.println("\nTRANSACTIONAL.ID RULES:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ┌────────────────────────────────────────────────────────────────┐");
        System.out.println("  │ Rule                                                           │");
        System.out.println("  ├────────────────────────────────────────────────────────────────┤");
        System.out.println("  │ 1. Must be UNIQUE per producer instance                        │");
        System.out.println("  │ 2. Must be STABLE across restarts (same ID = same producer)    │");
        System.out.println("  │ 3. If two producers use same ID → older one is FENCED          │");
        System.out.println("  │ 4. Format recommendation: \"app-name-partition-X\"               │");
        System.out.println("  └────────────────────────────────────────────────────────────────┘");
        System.out.println("");
        System.out.println("  FENCING EXAMPLE:");
        System.out.println("  Producer A (txn.id=\"transfer-processor-1\") ──► Running");
        System.out.println("  Producer B (txn.id=\"transfer-processor-1\") ──► Starts");
        System.out.println("  Producer A ──► ProducerFencedException (zombie fenced!)");
        System.out.println("");
        System.out.println("  This prevents zombie producers from corrupting transactions");
        System.out.println("");

        System.out.println("\nERROR HANDLING:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ┌─────────────────────────────┬───────────────────────────────────┐");
        System.out.println("  │ Exception                   │ Action                            │");
        System.out.println("  ├─────────────────────────────┼───────────────────────────────────┤");
        System.out.println("  │ ProducerFencedException     │ FATAL - Close producer, restart   │");
        System.out.println("  │ OutOfOrderSequenceException │ FATAL - Close producer, restart   │");
        System.out.println("  │ AuthorizationException      │ FATAL - Fix permissions           │");
        System.out.println("  │ KafkaException              │ abortTransaction(), retry         │");
        System.out.println("  │ Other exceptions            │ abortTransaction(), handle error  │");
        System.out.println("  └─────────────────────────────┴───────────────────────────────────┘");
        System.out.println("");

        System.out.println("\nCONSUMER SIDE - READ COMMITTED:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  // Consumer must set isolation level to see only committed messages");
        System.out.println("  props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, \"read_committed\");");
        System.out.println("");
        System.out.println("  ┌──────────────────────┬──────────────────────────────────────────┐");
        System.out.println("  │ Isolation Level      │ Behavior                                 │");
        System.out.println("  ├──────────────────────┼──────────────────────────────────────────┤");
        System.out.println("  │ read_uncommitted     │ See all messages (default)               │");
        System.out.println("  │ read_committed       │ See only committed transaction messages  │");
        System.out.println("  └──────────────────────┴──────────────────────────────────────────┘");
        System.out.println("");

        System.out.println("\nPERFORMANCE IMPACT:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ┌──────────────────────┬───────────────┬───────────────┐");
        System.out.println("  │ Aspect               │ Non-Txn       │ Transactional │");
        System.out.println("  ├──────────────────────┼───────────────┼───────────────┤");
        System.out.println("  │ Throughput           │ Higher        │ ~20-30% lower │");
        System.out.println("  │ Latency              │ Lower         │ Slightly higher│");
        System.out.println("  │ Broker CPU           │ Lower         │ Higher         │");
        System.out.println("  │ Coordinator overhead │ None          │ Yes            │");
        System.out.println("  └──────────────────────┴───────────────┴───────────────┘");
        System.out.println("");
        System.out.println("  Use transactions ONLY when atomicity is required!");
        System.out.println("");

        System.out.println("\nCONFIG COMPARISON:");
        System.out.println("-".repeat(70));
        System.out.println("| Config              | Demo06 (Idempotent) | Demo13 (Transactional) |");
        System.out.println("|---------------------|---------------------|------------------------|");
        System.out.println("| enable.idempotence  | true                | true (auto)            |");
        System.out.println("| transactional.id    | None                | Required               |");
        System.out.println("| Scope               | Single partition    | Multi-partition/topic  |");
        System.out.println("| Atomicity           | Per message         | Batch of messages      |");
        System.out.println("| API                 | send()              | begin/commit/abort     |");
        System.out.println("-".repeat(70));
    }
}