package com.example.producer;

import com.example.Transaction;
import com.example.JsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.math.BigDecimal;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DEMO 06: Idempotent Producer
 *
 * WHAT CHANGED FROM DEMO 05:
 * +---------------------------+-----------+------------------+
 * | Config                    | Demo 05   | Demo 06          |
 * +---------------------------+-----------+------------------+
 * | enable.idempotence        | false     | true             |
 * | max.in.flight.requests    | 5         | 5 (enforced)     |
 * | acks                      | all       | all (enforced)   |
 * | retries                   | 3         | MAX_INT (forced) |
 * +---------------------------+-----------+------------------+
 *
 * THE DUPLICATE PROBLEM:
 *
 * Without Idempotence:
 *
 *   Producer ─── send(msg1) ───> Broker (writes msg1)
 *       │                            │
 *       │                       ACK lost (network issue)
 *       │                            │
 *       │<─── timeout, no ACK ───────┘
 *       │
 *   Producer ─── retry(msg1) ──> Broker (writes msg1 AGAIN!)
 *       │                            │
 *       │<─── ACK ───────────────────┘
 *
 *   Result: msg1 written TWICE = DUPLICATE!
 *
 *
 * With Idempotence:
 *
 *   Producer ─── send(msg1, PID=1, SEQ=0) ───> Broker (writes, tracks PID+SEQ)
 *       │                                          │
 *       │                                     ACK lost
 *       │                                          │
 *       │<─── timeout ────────────────────────────┘
 *       │
 *   Producer ─── retry(msg1, PID=1, SEQ=0) ───> Broker (checks: already have PID=1,SEQ=0)
 *       │                                          │
 *       │                                     SKIP write, send ACK
 *       │<─── ACK ────────────────────────────────┘
 *
 *   Result: msg1 written ONCE = NO DUPLICATE!
 *
 *
 * HOW IT WORKS:
 * +------------------+---------------------------------------------------+
 * | Component        | Description                                       |
 * +------------------+---------------------------------------------------+
 * | Producer ID (PID)| Unique ID assigned to producer on init            |
 * | Sequence Number  | Per-partition counter, increments per message     |
 * | Broker Tracking  | Broker stores last (PID, SEQ) per partition       |
 * | Duplicate Check  | If PID+SEQ seen before, ACK without writing       |
 * +------------------+---------------------------------------------------+
 *
 * ENFORCED CONFIGS (when enable.idempotence=true):
 * - acks=all (required - must wait for all replicas)
 * - retries=MAX_INT (required - must retry forever)
 * - max.in.flight.requests.per.connection <= 5 (ordering guarantee)
 *
 * Banking Scenario: Fund transfers where duplicate debit is catastrophic
 */
public class Demo06_IdempotentProducer {

    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

        // CHANGED: Enable idempotence
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // These are AUTO-ENFORCED when idempotence=true, but shown for clarity
        // props.put(ProducerConfig.ACKS_CONFIG, "all");                          // Enforced
        // props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);           // Enforced
        // props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);    // Max allowed

        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(props);

        String topic = "fund-transfer-events";
        int messageCount = 1000;

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(messageCount);

        System.out.println("=".repeat(60));
        System.out.println("DEMO 06: Idempotent Producer");
        System.out.println("=".repeat(60));
        System.out.println("Config: enable.idempotence=true");
        System.out.println("Change: Duplicate prevention via PID + Sequence Number\n");

        System.out.println("ENFORCED SETTINGS (automatic when idempotence=true):");
        System.out.println("  acks                           = all");
        System.out.println("  retries                        = " + Integer.MAX_VALUE);
        System.out.println("  max.in.flight.requests.per.conn= 5 (max)\n");

        long startTime = System.currentTimeMillis();

        for (int i = 1; i <= messageCount; i++) {
            final int msgNum = i;

            Transaction txn = Transaction.builder()
                    .fromAccount("ACC" + (i % 100))
                    .toAccount("ACC" + ((i + 1) % 100))
                    .amount(new BigDecimal("100000.00"))
                    .type("FUND_TRANSFER")
                    .status("INITIATED")
                    .build();

            ProducerRecord<String, Transaction> record =
                    new ProducerRecord<>(topic, txn.getFromAccount(), txn);

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    successCount.incrementAndGet();

                    if (msgNum <= 5) {
                        System.out.printf("✓ Message %d [Key: %s] → Partition: %d, Offset: %d%n",
                                msgNum, record.key(), metadata.partition(), metadata.offset());
                    }
                } else {
                    failCount.incrementAndGet();
                    System.err.printf("✗ Message %d FAILED: %s%n", msgNum, exception.getMessage());
                }

                latch.countDown();
            });
        }

        latch.await();
        long duration = System.currentTimeMillis() - startTime;
        producer.close();

        System.out.println("\n" + "=".repeat(60));
        System.out.println("RESULTS");
        System.out.println("=".repeat(60));
        System.out.printf("Success/Failed  : %d / %d%n", successCount.get(), failCount.get());
        System.out.printf("Total Time      : %d ms%n", duration);
        System.out.printf("Throughput      : %d msg/sec%n", (messageCount * 1000 / duration));

        System.out.println("\n" + "-".repeat(60));
        System.out.println("IDEMPOTENCE MECHANISM:");
        System.out.println("-".repeat(60));
        System.out.println("");
        System.out.println("  ┌─────────────────────────────────────────────────────┐");
        System.out.println("  │                    PRODUCER                         │");
        System.out.println("  │  PID: 12345 (assigned on init)                      │");
        System.out.println("  │  Partition 0 SEQ: 0,1,2,3...                        │");
        System.out.println("  │  Partition 1 SEQ: 0,1,2,3...                        │");
        System.out.println("  └─────────────────────────────────────────────────────┘");
        System.out.println("                          │");
        System.out.println("                          ▼");
        System.out.println("  ┌─────────────────────────────────────────────────────┐");
        System.out.println("  │                    BROKER                           │");
        System.out.println("  │  Partition 0: Last (PID=12345, SEQ=3) → Skip if <=3 │");
        System.out.println("  │  Partition 1: Last (PID=12345, SEQ=2) → Skip if <=2 │");
        System.out.println("  └─────────────────────────────────────────────────────┘");
        System.out.println("");
        System.out.println("-".repeat(60));

        System.out.println("\n" + "-".repeat(60));
        System.out.println("DUPLICATE SCENARIO - BANKING EXAMPLE:");
        System.out.println("-".repeat(60));
        System.out.println("");
        System.out.println("  WITHOUT IDEMPOTENCE:");
        System.out.println("  ─────────────────────────────────────────────────────");
        System.out.println("  1. Transfer ₹1,00,000 from ACC001 to ACC002");
        System.out.println("  2. Broker writes, ACK lost in network");
        System.out.println("  3. Producer retries (thinks it failed)");
        System.out.println("  4. Broker writes AGAIN");
        System.out.println("  5. ACC001 debited ₹2,00,000 ← CATASTROPHIC!");
        System.out.println("");
        System.out.println("  WITH IDEMPOTENCE:");
        System.out.println("  ─────────────────────────────────────────────────────");
        System.out.println("  1. Transfer ₹1,00,000 (PID=123, SEQ=5)");
        System.out.println("  2. Broker writes, ACK lost");
        System.out.println("  3. Producer retries (PID=123, SEQ=5)");
        System.out.println("  4. Broker: 'Already have PID=123,SEQ=5' → SKIP");
        System.out.println("  5. ACC001 debited ₹1,00,000 ← CORRECT!");
        System.out.println("");
        System.out.println("-".repeat(60));

        System.out.println("\n" + "-".repeat(60));
        System.out.println("LIMITATIONS:");
        System.out.println("-".repeat(60));
        System.out.println("");
        System.out.println("  ✓ Guarantees: Exactly-once within SINGLE PARTITION");
        System.out.println("  ✗ Does NOT guarantee: Cross-partition exactly-once");
        System.out.println("  ✗ Does NOT guarantee: Exactly-once across producer restarts");
        System.out.println("");
        System.out.println("  For cross-partition exactly-once → Use TRANSACTIONS (Demo 13)");
        System.out.println("");
        System.out.println("-".repeat(60));

        System.out.println("\n" + "-".repeat(60));
        System.out.println("CONFIG COMPARISON:");
        System.out.println("-".repeat(60));
        System.out.println("| Config               | Demo05     | Demo06 (Idempotent) |");
        System.out.println("|----------------------|------------|---------------------|");
        System.out.println("| enable.idempotence   | false      | true                |");
        System.out.println("| acks                 | all        | all (enforced)      |");
        System.out.println("| retries              | 3          | MAX_INT (enforced)  |");
        System.out.println("| max.in.flight        | 5          | <=5 (enforced)      |");
        System.out.println("| Duplicate possible   | YES        | NO                  |");
        System.out.println("-".repeat(60));
    }
}