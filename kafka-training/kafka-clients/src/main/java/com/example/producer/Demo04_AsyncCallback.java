package com.example.producer;

import com.example.Transaction;
import com.example.JsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.math.BigDecimal;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * DEMO 04: Asynchronous Producer with Callback
 *
 * WHAT CHANGED FROM DEMO 03:
 * +------------------+---------------+------------------+
 * | Aspect           | Demo 03       | Demo 04          |
 * +------------------+---------------+------------------+
 * | Send style       | Blocking      | Non-blocking     |
 * | Result handling  | Future.get()  | Callback         |
 * | Throughput       | ~500/s        | ~10,000+/s       |
 * | Thread usage     | Blocked       | Free immediately |
 * +------------------+---------------+------------------+
 *
 * WHY ASYNC WITH CALLBACK:
 * - Producer thread doesn't wait
 * - Callback invoked when broker responds
 * - Best of both: High throughput + Delivery confirmation
 *
 * FLOW:
 *
 * SYNC (Demo 03):
 * send() -----> wait -----> wait -----> response -----> next send()
 *              [blocked]   [blocked]
 *
 * ASYNC (Demo 04):
 * send() -----> return immediately -----> send() -----> send() ...
 *    |                                       |             |
 *    +---> callback invoked later <----------+-------------+
 *
 * CALLBACK INTERFACE:
 * - onCompletion(RecordMetadata metadata, Exception exception)
 * - metadata != null → Success (partition, offset, timestamp)
 * - exception != null → Failure (handle error)
 *
 * Banking Scenario: High-volume payment notifications, real-time alerts
 */
public class Demo04_AsyncCallback {

    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

        // Same durability as Demo 03
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);

        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(props);

        String topic = "fund-transfer-events";
        int messageCount = 10000;

        // Tracking with thread-safe counters
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);
        AtomicLong totalLatency = new AtomicLong(0);
        CountDownLatch latch = new CountDownLatch(messageCount);

        System.out.println("=".repeat(60));
        System.out.println("DEMO 04: Asynchronous Producer with Callback");
        System.out.println("=".repeat(60));
        System.out.println("Config: acks=all, NON-BLOCKING send with Callback");
        System.out.println("Change: Producer thread not blocked, callback handles result\n");

        long startTime = System.currentTimeMillis();

        for (int i = 1; i <= messageCount; i++) {
            final int msgNum = i;
            final long sendTime = System.currentTimeMillis();

            Transaction txn = Transaction.builder()
                    .fromAccount("ACC" + (i % 1000))
                    .toAccount("ACC" + ((i + 1) % 1000))
                    .amount(new BigDecimal("100.00"))
                    .type("PAYMENT_ALERT")
                    .build();

            ProducerRecord<String, Transaction> record =
                    new ProducerRecord<>(topic, txn.getTransactionId(), txn);

            // CHANGED: Async send with Callback
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    long latency = System.currentTimeMillis() - sendTime;

                    if (exception == null) {
                        // SUCCESS
                        successCount.incrementAndGet();
                        totalLatency.addAndGet(latency);

                        if (msgNum <= 3) {
                            System.out.printf("✓ [Callback] Message %d → Partition: %d, Offset: %d, Latency: %d ms%n",
                                    msgNum, metadata.partition(), metadata.offset(), latency);
                        }
                    } else {
                        // FAILURE
                        failCount.incrementAndGet();
                        System.err.printf("✗ [Callback] Message %d FAILED: %s%n",
                                msgNum, exception.getMessage());
                        // Fallback
                    }

                    latch.countDown();
                }
            });

            // Producer thread continues immediately (non-blocking)
        }

        long sendCompleteTime = System.currentTimeMillis();
        long sendDuration = sendCompleteTime - startTime;

        System.out.printf("%nAll %d sends initiated in %d ms (non-blocking)%n", messageCount, sendDuration);
        System.out.println("Waiting for all callbacks to complete...\n");

        // Wait for all callbacks
        latch.await();

        long totalDuration = System.currentTimeMillis() - startTime;
        producer.close();

        int success = successCount.get();
        int failed = failCount.get();

        System.out.println("=".repeat(60));
        System.out.println("RESULTS");
        System.out.println("=".repeat(60));
        System.out.printf("Success/Failed     : %d / %d%n", success, failed);
        System.out.printf("Send Time (async)  : %d ms%n", sendDuration);
        System.out.printf("Total Time         : %d ms%n", totalDuration);
        System.out.printf("Send Throughput    : %d msg/sec (non-blocking)%n", (messageCount * 1000 / sendDuration));
        System.out.printf("Actual Throughput  : %d msg/sec (with acks)%n", (messageCount * 1000 / totalDuration));
        System.out.printf("Avg Ack Latency    : %.2f ms%n", totalLatency.get() / (double) success);

        System.out.println("\n" + "-".repeat(60));
        System.out.println("COMPARISON: Sync vs Async (both acks=all)");
        System.out.println("-".repeat(60));
        System.out.println("| Metric           | Demo03 Sync  | Demo04 Async  |");
        System.out.println("|------------------|--------------|---------------|");
        System.out.println("| Messages         | 1,000        | 10,000        |");
        System.out.println("| Send Style       | Blocking     | Non-blocking  |");
        System.out.println("| Thread blocked   | Yes          | No            |");
        System.out.println("| Throughput       | ~500/s       | ~10,000+/s    |");
        System.out.println("| Durability       | acks=all     | acks=all      |");
        System.out.println("| Error handling   | try-catch    | Callback      |");
        System.out.println("-".repeat(60));

        System.out.println("\n" + "-".repeat(60));
        System.out.println("CALLBACK PATTERN:");
        System.out.println("-".repeat(60));
        System.out.println("");
        System.out.println("  producer.send(record, (metadata, exception) -> {");
        System.out.println("      if (exception == null) {");
        System.out.println("          // SUCCESS: metadata.partition(), metadata.offset()");
        System.out.println("      } else {");
        System.out.println("          // FAILURE: log, retry, or send to DLQ");
        System.out.println("      }");
        System.out.println("  });");
        System.out.println("");
        System.out.println("-".repeat(60));

        System.out.println("\n" + "-".repeat(60));
        System.out.println("WHEN TO USE ASYNC:");
        System.out.println("-".repeat(60));
        System.out.println("✓ High-throughput requirements");
        System.out.println("✓ Don't want to block application thread");
        System.out.println("✓ Can handle results asynchronously");
        System.out.println("✓ Batch processing, event streaming");
        System.out.println("");
        System.out.println("WHEN TO USE SYNC:");
        System.out.println("✗ Need immediate confirmation before proceeding");
        System.out.println("✗ Sequential processing required");
        System.out.println("✗ Simple error handling in same thread");
        System.out.println("-".repeat(60));
    }
}