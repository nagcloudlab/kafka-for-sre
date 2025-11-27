package com.example.producer;

import com.example.Transaction;
import com.example.JsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.math.BigDecimal;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DEMO 05: Retries & Error Handling
 *
 * WHAT CHANGED FROM DEMO 04:
 * +------------------------+-----------+-----------+
 * | Config                 | Demo 04   | Demo 05   |
 * +------------------------+-----------+-----------+
 * | retries                | 0         | 3         |
 * | retry.backoff.ms       | (default) | 100       |
 * | delivery.timeout.ms    | (default) | 30000     |
 * | request.timeout.ms     | (default) | 10000     |
 * +------------------------+-----------+-----------+
 *
 * RETRY CONFIGS EXPLAINED:
 * +------------------------+----------+------------------------------------------+
 * | Config                 | Default  | Description                              |
 * +------------------------+----------+------------------------------------------+
 * | retries                | MAX_INT  | Max retry attempts (2147483647)          |
 * | retry.backoff.ms       | 100      | Wait time between retries                |
 * | retry.backoff.max.ms   | 1000     | Max backoff (exponential growth cap)     |
 * | delivery.timeout.ms    | 120000   | Total time for send (including retries)  |
 * | request.timeout.ms     | 30000    | Time to wait for single request          |
 * +------------------------+----------+------------------------------------------+
 *
 * TIMEOUT RELATIONSHIP:
 * delivery.timeout.ms >= request.timeout.ms + (retries × retry.backoff.ms)
 *
 * RETRIABLE vs NON-RETRIABLE ERRORS:
 * +---------------------------+-------------+--------------------------------+
 * | Error Type                | Retriable?  | Example                        |
 * +---------------------------+-------------+--------------------------------+
 * | NetworkException          | Yes         | Broker temporarily unavailable |
 * | NotLeaderForPartition     | Yes         | Leader election in progress    |
 * | TimeoutException          | Yes         | Request timed out              |
 * | NotEnoughReplicasException| Yes         | ISR below min.insync.replicas  |
 * | InvalidTopicException     | No          | Topic doesn't exist            |
 * | RecordTooLargeException   | No          | Message exceeds max size       |
 * | SerializationException    | No          | Serialization failed           |
 * | AuthorizationException    | No          | No permission                  |
 * +---------------------------+-------------+--------------------------------+
 *
 * Banking Scenario: Resilient fund transfer with automatic recovery
 */
public class Demo05_RetriesErrorHandling {

    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

        // Durability
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // CHANGED: Retry configuration
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
        props.put(ProducerConfig.RETRY_BACKOFF_MAX_MS_CONFIG, 1000);

        // Timeout configuration
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);      // 10s per request
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30000);     // 30s total

        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(props);

        String topic = "fund-transfer-events";
        int messageCount = 1000;

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger retriableFailCount = new AtomicInteger(0);
        AtomicInteger nonRetriableFailCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(messageCount);

        System.out.println("=".repeat(60));
        System.out.println("DEMO 05: Retries & Error Handling");
        System.out.println("=".repeat(60));
        System.out.println("Config: acks=all, retries=3, retry.backoff.ms=100");
        System.out.println("Change: Automatic retries for transient failures\n");

        System.out.println("RETRY CONFIG:");
        System.out.println("  retries              = 3");
        System.out.println("  retry.backoff.ms     = 100 ms");
        System.out.println("  retry.backoff.max.ms = 1000 ms");
        System.out.println("  request.timeout.ms   = 10000 ms");
        System.out.println("  delivery.timeout.ms  = 30000 ms\n");

        long startTime = System.currentTimeMillis();

        for (int i = 1; i <= messageCount; i++) {
            final int msgNum = i;

            Transaction txn = Transaction.builder()
                    .fromAccount("ACC" + (i % 100))
                    .toAccount("ACC" + ((i + 1) % 100))
                    .amount(new BigDecimal("25000.00"))
                    .type("FUND_TRANSFER")
                    .build();

            ProducerRecord<String, Transaction> record =
                    new ProducerRecord<>(topic, txn.getTransactionId(), txn);

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    successCount.incrementAndGet();

                    if (msgNum <= 3) {
                        System.out.printf("✓ Message %d → Partition: %d, Offset: %d%n",
                                msgNum, metadata.partition(), metadata.offset());
                    }
                } else {
                    // Classify error type
                    if (exception instanceof RetriableException) {
                        retriableFailCount.incrementAndGet();
                        System.err.printf("✗ Message %d FAILED (Retriable, exhausted retries): %s%n",
                                msgNum, exception.getClass().getSimpleName());
                    } else {
                        nonRetriableFailCount.incrementAndGet();
                        System.err.printf("✗ Message %d FAILED (Non-retriable): %s - %s%n",
                                msgNum, exception.getClass().getSimpleName(), exception.getMessage());
                    }

                    // Error handling strategy
                    handleFailedMessage(txn, exception);
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
        System.out.printf("Success              : %d%n", successCount.get());
        System.out.printf("Retriable Failures   : %d (exhausted retries)%n", retriableFailCount.get());
        System.out.printf("Non-Retriable Fails  : %d%n", nonRetriableFailCount.get());
        System.out.printf("Total Time           : %d ms%n", duration);
        System.out.printf("Throughput           : %d msg/sec%n", (messageCount * 1000 / duration));

        System.out.println("\n" + "-".repeat(60));
        System.out.println("RETRY TIMELINE EXAMPLE:");
        System.out.println("-".repeat(60));
        System.out.println("");
        System.out.println("  Attempt 1 ──> FAIL (NetworkException)");
        System.out.println("       │");
        System.out.println("       └── wait 100ms (retry.backoff.ms)");
        System.out.println("              │");
        System.out.println("  Attempt 2 ──> FAIL (NotLeaderForPartition)");
        System.out.println("       │");
        System.out.println("       └── wait 200ms (exponential backoff)");
        System.out.println("              │");
        System.out.println("  Attempt 3 ──> FAIL (TimeoutException)");
        System.out.println("       │");
        System.out.println("       └── wait 400ms (capped at retry.backoff.max.ms)");
        System.out.println("              │");
        System.out.println("  Attempt 4 ──> SUCCESS or GIVE UP");
        System.out.println("");
        System.out.println("-".repeat(60));

        System.out.println("\n" + "-".repeat(60));
        System.out.println("ERROR HANDLING STRATEGIES:");
        System.out.println("-".repeat(60));
        System.out.println("");
        System.out.println("1. RETRY (automatic via config):");
        System.out.println("   - NetworkException, TimeoutException");
        System.out.println("   - NotLeaderForPartition, NotEnoughReplicas");
        System.out.println("");
        System.out.println("2. DEAD LETTER QUEUE (manual in callback):");
        System.out.println("   - Send failed message to 'fund-transfer-dlq' topic");
        System.out.println("   - Process later or alert operations");
        System.out.println("");
        System.out.println("3. LOG & ALERT (non-retriable):");
        System.out.println("   - RecordTooLargeException → Fix message size");
        System.out.println("   - SerializationException → Fix data format");
        System.out.println("   - AuthorizationException → Fix permissions");
        System.out.println("");
        System.out.println("-".repeat(60));

        System.out.println("\n" + "-".repeat(60));
        System.out.println("CONFIG COMPARISON:");
        System.out.println("-".repeat(60));
        System.out.println("| Config              | Demo04   | Demo05    | Production  |");
        System.out.println("|---------------------|----------|-----------|-------------|");
        System.out.println("| retries             | 0        | 3         | MAX_INT     |");
        System.out.println("| retry.backoff.ms    | 100      | 100       | 100         |");
        System.out.println("| delivery.timeout.ms | 120000   | 30000     | 120000      |");
        System.out.println("| request.timeout.ms  | 30000    | 10000     | 30000       |");
        System.out.println("-".repeat(60));
    }

    /**
     * Error handling strategy for failed messages
     */
    private static void handleFailedMessage(Transaction txn, Exception exception) {
        // Strategy 1: Log for monitoring
        // logger.error("Failed transaction: {}", txn.getTransactionId(), exception);

        // Strategy 2: Send to Dead Letter Queue
        // dlqProducer.send(new ProducerRecord<>("fund-transfer-dlq", txn));

        // Strategy 3: Store in database for retry
        // failedTransactionRepository.save(txn, exception.getMessage());

        // Strategy 4: Alert operations team
        // alertService.sendAlert("KAFKA_SEND_FAILURE", txn.getTransactionId());
    }
}