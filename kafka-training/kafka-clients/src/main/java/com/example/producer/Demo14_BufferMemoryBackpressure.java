package com.example.producer;

import com.example.Transaction;
import com.example.JsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.math.BigDecimal;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * DEMO 14: Buffer Memory & Backpressure
 *
 * WHAT CHANGED FROM DEMO 13:
 * +---------------------------+-------------------+---------------------+
 * | Aspect                    | Demo 13           | Demo 14             |
 * +---------------------------+-------------------+---------------------+
 * | Focus                     | Exactly-once      | Stability under load|
 * | buffer.memory             | Default 32MB      | Configurable        |
 * | max.block.ms              | Default 60s       | Configurable        |
 * | Backpressure handling     | None              | Explicit            |
 * +---------------------------+-------------------+---------------------+
 *
 * BUFFER MEMORY CONCEPT:
 *
 * ┌─────────────────────────────────────────────────────────────────────┐
 * │                     PRODUCER MEMORY LAYOUT                          │
 * ├─────────────────────────────────────────────────────────────────────┤
 * │                                                                     │
 * │  buffer.memory = 64 MB (total for all partitions)                   │
 * │  ┌─────────────────────────────────────────────────────────────┐   │
 * │  │ Partition 0 │ Partition 1 │ Partition 2 │ ... │ Free Space  │   │
 * │  │ Batch Queue │ Batch Queue │ Batch Queue │     │             │   │
 * │  │ [████████]  │ [██████]    │ [████]      │     │ [░░░░░░░░]  │   │
 * │  └─────────────────────────────────────────────────────────────┘   │
 * │                                                                     │
 * │  When buffer.memory exhausted:                                      │
 * │  → send() BLOCKS for up to max.block.ms                             │
 * │  → If still full after max.block.ms → TimeoutException              │
 * │                                                                     │
 * └─────────────────────────────────────────────────────────────────────┘
 *
 * BACKPRESSURE SCENARIOS:
 *
 * 1. SLOW BROKER:
 *    Producer sends faster than broker can accept
 *    → Buffer fills up → Backpressure
 *
 * 2. NETWORK ISSUES:
 *    Temporary network problems
 *    → Retries accumulate → Buffer fills → Backpressure
 *
 * 3. BURST TRAFFIC:
 *    Sudden spike in message volume
 *    → Buffer fills quickly → Backpressure
 *
 * Banking Scenario: Handle payment spikes during salary day, festival sales
 */
public class Demo14_BufferMemoryBackpressure {

    public static void main(String[] args) throws InterruptedException {

        System.out.println("=".repeat(70));
        System.out.println("DEMO 14: Buffer Memory & Backpressure");
        System.out.println("=".repeat(70));
        System.out.println("Handling producer stability under high load\n");

        // Part 1: Default buffer settings
        runDefaultBufferDemo();

        // Part 2: Small buffer (trigger backpressure)
        runSmallBufferDemo();

        // Part 3: Backpressure handling strategies
        runBackpressureStrategiesDemo();

        printAnalysis();
    }

    private static void runDefaultBufferDemo() throws InterruptedException {
        System.out.println("-".repeat(70));
        System.out.println("PART 1: Default Buffer Settings (32MB buffer, 60s block)");
        System.out.println("-".repeat(70));
        System.out.println("");

        Properties props = createBaseProperties();
        // Default: buffer.memory=33554432 (32MB), max.block.ms=60000 (60s)

        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(props);

        String topic = "fund-transfer-events";
        int messageCount = 10000;

        AtomicInteger success = new AtomicInteger(0);
        AtomicInteger failed = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(messageCount);

        System.out.printf("  Sending %d messages with default buffer settings...%n%n", messageCount);

        long startTime = System.currentTimeMillis();

        for (int i = 1; i <= messageCount; i++) {
            Transaction txn = Transaction.builder()
                    .fromAccount("ACC" + String.format("%05d", i))
                    .toAccount("MERCHANT-001")
                    .amount(new BigDecimal("1000.00"))
                    .type("PAYMENT")
                    .build();

            ProducerRecord<String, Transaction> record =
                    new ProducerRecord<>(topic, txn.getFromAccount(), txn);

            try {
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        success.incrementAndGet();
                    } else {
                        failed.incrementAndGet();
                    }
                    latch.countDown();
                });
            } catch (Exception e) {
                failed.incrementAndGet();
                latch.countDown();
            }
        }

        long sendTime = System.currentTimeMillis() - startTime;
        System.out.printf("  All sends initiated in: %d ms%n", sendTime);

        latch.await();
        long totalTime = System.currentTimeMillis() - startTime;

        producer.close();

        System.out.printf("  Total time: %d ms%n", totalTime);
        System.out.printf("  Success: %d, Failed: %d%n", success.get(), failed.get());
        System.out.printf("  Throughput: %d msg/sec%n%n", (messageCount * 1000L) / totalTime);
    }

    private static void runSmallBufferDemo() throws InterruptedException {
        System.out.println("-".repeat(70));
        System.out.println("PART 2: Small Buffer (Trigger Backpressure)");
        System.out.println("-".repeat(70));
        System.out.println("");

        Properties props = createBaseProperties();

        // SMALL BUFFER - will trigger backpressure quickly
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 65536);    // 64KB only!
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);      // 5 second timeout
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);       // 16KB batch

        System.out.println("  Config:");
        System.out.println("    buffer.memory = 64 KB (very small!)");
        System.out.println("    max.block.ms  = 5000 ms");
        System.out.println("    batch.size    = 16 KB\n");

        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(props);

        String topic = "fund-transfer-events";
        int messageCount = 5000;

        AtomicInteger success = new AtomicInteger(0);
        AtomicInteger failed = new AtomicInteger(0);
        AtomicInteger blocked = new AtomicInteger(0);
        AtomicLong totalBlockTime = new AtomicLong(0);
        CountDownLatch latch = new CountDownLatch(messageCount);

        System.out.printf("  Sending %d messages with tiny buffer (expect blocking)...%n%n", messageCount);

        long startTime = System.currentTimeMillis();

        for (int i = 1; i <= messageCount; i++) {
            Transaction txn = Transaction.builder()
                    .fromAccount("ACC" + String.format("%05d", i))
                    .toAccount("MERCHANT-001")
                    .amount(new BigDecimal("1000.00"))
                    .type("BURST_PAYMENT")
                    .build();

            ProducerRecord<String, Transaction> record =
                    new ProducerRecord<>(topic, txn.getFromAccount(), txn);

            long sendStart = System.currentTimeMillis();
            try {
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        success.incrementAndGet();
                    } else {
                        failed.incrementAndGet();
                    }
                    latch.countDown();
                });

                long sendDuration = System.currentTimeMillis() - sendStart;
                if (sendDuration > 100) {
                    blocked.incrementAndGet();
                    totalBlockTime.addAndGet(sendDuration);
                    if (blocked.get() <= 5) {
                        System.out.printf("  [BLOCKED] Message %d blocked for %d ms (buffer full)%n",
                                i, sendDuration);
                    }
                }

            } catch (TimeoutException e) {
                failed.incrementAndGet();
                latch.countDown();
                System.out.printf("  [TIMEOUT] Message %d - buffer full for > 5 seconds%n", i);
            } catch (Exception e) {
                failed.incrementAndGet();
                latch.countDown();
            }
        }

        long sendTime = System.currentTimeMillis() - startTime;
        System.out.printf("%n  All sends initiated in: %d ms%n", sendTime);

        latch.await();
        long totalTime = System.currentTimeMillis() - startTime;

        producer.close();

        System.out.printf("  Total time: %d ms%n", totalTime);
        System.out.printf("  Success: %d, Failed: %d%n", success.get(), failed.get());
        System.out.printf("  Blocked calls: %d (total block time: %d ms)%n",
                blocked.get(), totalBlockTime.get());
        System.out.printf("  Throughput: %d msg/sec%n%n", (messageCount * 1000L) / totalTime);
    }

    private static void runBackpressureStrategiesDemo() throws InterruptedException {
        System.out.println("-".repeat(70));
        System.out.println("PART 3: Backpressure Handling Strategies");
        System.out.println("-".repeat(70));
        System.out.println("");

        // Strategy 1: Large buffer with monitoring
        System.out.println("  STRATEGY 1: Large Buffer + Monitoring");
        System.out.println("  ─────────────────────────────────────");
        runLargeBufferWithMonitoring();

        // Strategy 2: Non-blocking with fallback
        System.out.println("\n  STRATEGY 2: Non-Blocking with Fallback Queue");
        System.out.println("  ────────────────────────────────────────────");
        runNonBlockingWithFallback();

        // Strategy 3: Rate limiting
        System.out.println("\n  STRATEGY 3: Rate Limiting");
        System.out.println("  ─────────────────────────");
        runRateLimitedProducer();
    }

    private static void runLargeBufferWithMonitoring() throws InterruptedException {
        Properties props = createBaseProperties();
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 134217728);  // 128MB
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30000);       // 30 seconds

        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(props);

        String topic = "fund-transfer-events";
        int messageCount = 5000;
        CountDownLatch latch = new CountDownLatch(messageCount);
        AtomicInteger success = new AtomicInteger(0);

        System.out.println("    buffer.memory = 128 MB");
        System.out.println("    max.block.ms  = 30000 ms");

        long startTime = System.currentTimeMillis();

        for (int i = 1; i <= messageCount; i++) {
            Transaction txn = Transaction.builder()
                    .fromAccount("LARGE-ACC" + i)
                    .toAccount("MERCHANT")
                    .amount(new BigDecimal("100.00"))
                    .type("PAYMENT")
                    .build();

            producer.send(new ProducerRecord<>(topic, txn.getFromAccount(), txn),
                    (metadata, exception) -> {
                        if (exception == null) success.incrementAndGet();
                        latch.countDown();
                    });
        }

        latch.await();
        long duration = System.currentTimeMillis() - startTime;
        producer.close();

        System.out.printf("    Result: %d/%d success in %d ms%n", success.get(), messageCount, duration);
    }

    private static void runNonBlockingWithFallback() throws InterruptedException {
        Properties props = createBaseProperties();
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 65536);     // 64KB (small)
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 0);          // DON'T BLOCK!

        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(props);

        String topic = "fund-transfer-events";
        int messageCount = 1000;

        AtomicInteger sentToKafka = new AtomicInteger(0);
        AtomicInteger sentToFallback = new AtomicInteger(0);

        System.out.println("    buffer.memory = 64 KB");
        System.out.println("    max.block.ms  = 0 (non-blocking!)");
        System.out.println("    Fallback: In-memory queue (simulated)");

        for (int i = 1; i <= messageCount; i++) {
            Transaction txn = Transaction.builder()
                    .fromAccount("NB-ACC" + i)
                    .toAccount("MERCHANT")
                    .amount(new BigDecimal("50.00"))
                    .type("PAYMENT")
                    .build();

            try {
                producer.send(new ProducerRecord<>(topic, txn.getFromAccount(), txn));
                sentToKafka.incrementAndGet();
            } catch (Exception e) {
                // Buffer full - send to fallback queue
                sendToFallbackQueue(txn);
                sentToFallback.incrementAndGet();
            }
        }

        producer.close();

        System.out.printf("    Result: %d to Kafka, %d to fallback queue%n",
                sentToKafka.get(), sentToFallback.get());
    }

    private static void sendToFallbackQueue(Transaction txn) {
        // Simulated fallback: In real scenario, use:
        // - In-memory queue (ConcurrentLinkedQueue)
        // - Local file
        // - Database
        // - Another messaging system
    }

    private static void runRateLimitedProducer() throws InterruptedException {
        Properties props = createBaseProperties();
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);  // 64MB

        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(props);

        String topic = "fund-transfer-events";
        int messageCount = 1000;
        int rateLimit = 500;  // 500 msg/sec

        AtomicInteger success = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(messageCount);

        System.out.printf("    Rate limit: %d msg/sec%n", rateLimit);

        long startTime = System.currentTimeMillis();
        long intervalMs = 1000 / rateLimit;  // Time between messages

        for (int i = 1; i <= messageCount; i++) {
            Transaction txn = Transaction.builder()
                    .fromAccount("RL-ACC" + i)
                    .toAccount("MERCHANT")
                    .amount(new BigDecimal("25.00"))
                    .type("PAYMENT")
                    .build();

            producer.send(new ProducerRecord<>(topic, txn.getFromAccount(), txn),
                    (metadata, exception) -> {
                        if (exception == null) success.incrementAndGet();
                        latch.countDown();
                    });

            // Rate limiting delay
            Thread.sleep(intervalMs);
        }

        latch.await();
        long duration = System.currentTimeMillis() - startTime;
        producer.close();

        System.out.printf("    Result: %d/%d success in %d ms%n", success.get(), messageCount, duration);
        System.out.printf("    Actual rate: %d msg/sec%n", (messageCount * 1000L) / duration);
    }

    private static Properties createBaseProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        return props;
    }

    private static void printAnalysis() {
        System.out.println("=".repeat(70));
        System.out.println("BUFFER MEMORY & BACKPRESSURE ANALYSIS");
        System.out.println("=".repeat(70));

        System.out.println("\nBUFFER MEMORY CONFIGS:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ┌─────────────────────┬─────────────┬───────────────────────────────┐");
        System.out.println("  │ Config              │ Default     │ Description                   │");
        System.out.println("  ├─────────────────────┼─────────────┼───────────────────────────────┤");
        System.out.println("  │ buffer.memory       │ 33554432    │ Total bytes for buffering     │");
        System.out.println("  │                     │ (32 MB)     │ across all partitions         │");
        System.out.println("  ├─────────────────────┼─────────────┼───────────────────────────────┤");
        System.out.println("  │ max.block.ms        │ 60000       │ Max time send() blocks when   │");
        System.out.println("  │                     │ (60 sec)    │ buffer full                   │");
        System.out.println("  ├─────────────────────┼─────────────┼───────────────────────────────┤");
        System.out.println("  │ batch.size          │ 16384       │ Max bytes per batch           │");
        System.out.println("  │                     │ (16 KB)     │ (per partition)               │");
        System.out.println("  └─────────────────────┴─────────────┴───────────────────────────────┘");
        System.out.println("");

        System.out.println("\nBUFFER MEMORY FLOW:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │                    PRODUCER buffer.memory                       │");
        System.out.println("  │                         (64 MB)                                 │");
        System.out.println("  ├─────────────────────────────────────────────────────────────────┤");
        System.out.println("  │                                                                 │");
        System.out.println("  │  ┌───────────┐ ┌───────────┐ ┌───────────┐                     │");
        System.out.println("  │  │Partition 0│ │Partition 1│ │Partition 2│  ...                │");
        System.out.println("  │  │           │ │           │ │           │                     │");
        System.out.println("  │  │ [Batch 1] │ │ [Batch 1] │ │ [Batch 1] │                     │");
        System.out.println("  │  │ [Batch 2] │ │ [Batch 2] │ │           │                     │");
        System.out.println("  │  │ [Batch 3] │ │           │ │           │                     │");
        System.out.println("  │  └───────────┘ └───────────┘ └───────────┘                     │");
        System.out.println("  │                                                                 │");
        System.out.println("  │  [████████████████████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░]   │");
        System.out.println("  │   Used: 40 MB                           Free: 24 MB            │");
        System.out.println("  │                                                                 │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("");

        System.out.println("\nBACKPRESSURE SCENARIOS:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  SCENARIO 1: Buffer has space");
        System.out.println("  ─────────────────────────────");
        System.out.println("  send() → Returns immediately → Message buffered");
        System.out.println("");
        System.out.println("  SCENARIO 2: Buffer full, max.block.ms not reached");
        System.out.println("  ────────────────────────────────────────────────");
        System.out.println("  send() → BLOCKS → Waits for buffer space → Continues");
        System.out.println("");
        System.out.println("  SCENARIO 3: Buffer full, max.block.ms exceeded");
        System.out.println("  ─────────────────────────────────────────────");
        System.out.println("  send() → BLOCKS → Timeout → TimeoutException!");
        System.out.println("");
        System.out.println("  Timeline:");
        System.out.println("  ┌────────────────────────────────────────────────────────────┐");
        System.out.println("  │ 0ms        10s         30s         60s                     │");
        System.out.println("  │ │          │           │           │                       │");
        System.out.println("  │ send()─────►BLOCKING───►BLOCKING───►TimeoutException       │");
        System.out.println("  │ [buffer full]                      [max.block.ms=60000]    │");
        System.out.println("  └────────────────────────────────────────────────────────────┘");
        System.out.println("");

        System.out.println("\nBACKPRESSURE HANDLING STRATEGIES:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │ STRATEGY 1: Large Buffer                                        │");
        System.out.println("  ├─────────────────────────────────────────────────────────────────┤");
        System.out.println("  │ buffer.memory = 128MB or more                                   │");
        System.out.println("  │ Absorb traffic spikes without blocking                          │");
        System.out.println("  │ Trade-off: More memory usage                                    │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │ STRATEGY 2: Non-Blocking with Fallback                          │");
        System.out.println("  ├─────────────────────────────────────────────────────────────────┤");
        System.out.println("  │ max.block.ms = 0                                                │");
        System.out.println("  │ Catch exception → Send to fallback queue                        │");
        System.out.println("  │ Process fallback queue later                                    │");
        System.out.println("  │ Trade-off: More complex, need fallback processing               │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │ STRATEGY 3: Rate Limiting                                       │");
        System.out.println("  ├─────────────────────────────────────────────────────────────────┤");
        System.out.println("  │ Limit send rate to sustainable throughput                       │");
        System.out.println("  │ Use token bucket or leaky bucket algorithm                      │");
        System.out.println("  │ Trade-off: Increased latency during spikes                      │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │ STRATEGY 4: Monitoring + Auto-scaling                           │");
        System.out.println("  ├─────────────────────────────────────────────────────────────────┤");
        System.out.println("  │ Monitor: buffer-available-bytes metric                          │");
        System.out.println("  │ Alert when < 20% free                                           │");
        System.out.println("  │ Auto-scale producers or increase partitions                     │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("");

        System.out.println("\nBANKING SCENARIO - SALARY DAY SPIKE:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  Normal day:  10,000 transactions/sec");
        System.out.println("  Salary day: 100,000 transactions/sec (10x spike!)");
        System.out.println("");
        System.out.println("  RECOMMENDED CONFIG:");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │ buffer.memory     = 256 MB (handle 10x spike)                   │");
        System.out.println("  │ max.block.ms      = 30000 (30 sec tolerance)                    │");
        System.out.println("  │ batch.size        = 64 KB (larger batches)                      │");
        System.out.println("  │ linger.ms         = 50 (wait for batch fill)                    │");
        System.out.println("  │ compression.type  = lz4 (reduce memory per message)             │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("");
        System.out.println("  FALLBACK PLAN:");
        System.out.println("  1. If buffer > 80% full → Alert operations");
        System.out.println("  2. If buffer > 95% full → Redirect to fallback queue");
        System.out.println("  3. Process fallback queue after spike subsides");
        System.out.println("");

        System.out.println("\nMONITORING METRICS:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  JMX Metrics to monitor:");
        System.out.println("");
        System.out.println("  ┌─────────────────────────────────┬────────────────────────────────┐");
        System.out.println("  │ Metric                          │ Alert Threshold                │");
        System.out.println("  ├─────────────────────────────────┼────────────────────────────────┤");
        System.out.println("  │ buffer-available-bytes          │ < 20% of buffer.memory         │");
        System.out.println("  │ buffer-exhausted-total          │ Any increase                   │");
        System.out.println("  │ bufferpool-wait-time-total      │ > 1000 ms                      │");
        System.out.println("  │ record-queue-time-avg           │ > 500 ms                       │");
        System.out.println("  │ batch-size-avg                  │ Near batch.size (good)         │");
        System.out.println("  └─────────────────────────────────┴────────────────────────────────┘");
        System.out.println("");

        System.out.println("\nBUFFER SIZING FORMULA:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  buffer.memory = ");
        System.out.println("      (expected_throughput × avg_message_size × buffer_seconds)");
        System.out.println("      × safety_factor");
        System.out.println("");
        System.out.println("  Example:");
        System.out.println("  - Expected throughput: 10,000 msg/sec");
        System.out.println("  - Average message size: 500 bytes");
        System.out.println("  - Buffer for 10 seconds: 10 sec");
        System.out.println("  - Safety factor: 2x (for spikes)");
        System.out.println("");
        System.out.println("  buffer.memory = 10,000 × 500 × 10 × 2 = 100,000,000 bytes ≈ 100 MB");
        System.out.println("");

        System.out.println("\nCONFIG COMPARISON:");
        System.out.println("-".repeat(70));
        System.out.println("| Config          | Default     | Low Latency  | High Throughput |");
        System.out.println("|-----------------|-------------|--------------|-----------------|");
        System.out.println("| buffer.memory   | 32 MB       | 64 MB        | 256 MB          |");
        System.out.println("| max.block.ms    | 60000       | 5000         | 120000          |");
        System.out.println("| batch.size      | 16 KB       | 16 KB        | 64 KB           |");
        System.out.println("| linger.ms       | 0           | 0            | 50-100          |");
        System.out.println("-".repeat(70));
    }
}