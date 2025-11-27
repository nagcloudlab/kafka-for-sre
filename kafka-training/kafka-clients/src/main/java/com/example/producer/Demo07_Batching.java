package com.example.producer;

import com.example.Transaction;
import com.example.JsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.math.BigDecimal;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DEMO 07: Batching Configuration
 *
 * WHAT CHANGED FROM DEMO 06:
 * +---------------------------+-----------+-----------+
 * | Config                    | Demo 06   | Demo 07   |
 * +---------------------------+-----------+-----------+
 * | batch.size                | 16384     | 32768     |
 * | linger.ms                 | 0         | 20        |
 * | buffer.memory             | 33554432  | 67108864  |
 * +---------------------------+-----------+-----------+
 *
 * BATCHING CONCEPT:
 *
 * Without Batching (linger.ms=0):
 * ┌─────┐   ┌─────┐   ┌─────┐   ┌─────┐
 * │ M1  │──►│ M2  │──►│ M3  │──►│ M4  │──►  4 Network calls
 * └─────┘   └─────┘   └─────┘   └─────┘
 *
 * With Batching (linger.ms=20):
 * ┌─────────────────────────┐
 * │ M1 │ M2 │ M3 │ M4 │ ... │──►  1 Network call
 * └─────────────────────────┘
 *         BATCH
 *
 * BATCHING CONFIGS EXPLAINED:
 * +------------------+----------+------------------------------------------------+
 * | Config           | Default  | Description                                    |
 * +------------------+----------+------------------------------------------------+
 * | batch.size       | 16384    | Max bytes per batch (per partition)            |
 * | linger.ms        | 0        | Time to wait for batch to fill                 |
 * | buffer.memory    | 33554432 | Total memory for all unsent batches (32MB)     |
 * +------------------+----------+------------------------------------------------+
 *
 * WHEN BATCH IS SENT:
 * 1. batch.size reached (batch full)
 * 2. linger.ms elapsed (time limit)
 * 3. Another batch to same broker is ready
 * 4. flush() or close() called
 *
 * TRADE-OFFS:
 * +-------------------+---------------------+----------------------+
 * | Setting           | Throughput          | Latency              |
 * +-------------------+---------------------+----------------------+
 * | linger.ms=0       | Lower (many calls)  | Lowest (immediate)   |
 * | linger.ms=5-20    | Higher (batched)    | Slight increase      |
 * | linger.ms=100+    | Highest             | Noticeable delay     |
 * +-------------------+---------------------+----------------------+
 *
 * Banking Scenario: Bulk transaction processing, end-of-day settlements
 */
public class Demo07_Batching {

    public static void main(String[] args) throws InterruptedException {

        System.out.println("=".repeat(70));
        System.out.println("DEMO 07: Batching Configuration");
        System.out.println("=".repeat(70));
        System.out.println("Comparing different batch settings\n");

        // Run 3 scenarios
        runScenario("Scenario A: No Batching", 0, 16384);
        runScenario("Scenario B: Moderate Batching", 20, 32768);
        runScenario("Scenario C: Aggressive Batching", 100, 65536);

        printComparison();
    }

    private static long runScenario(String name, int lingerMs, int batchSize)
            throws InterruptedException {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

        // Idempotent (from Demo 06)
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // CHANGED: Batching configuration
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);  // 64MB

        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(props);

        String topic = "fund-transfer-events";
        int messageCount = 10000;

        AtomicInteger successCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(messageCount);

        System.out.println("-".repeat(70));
        System.out.printf("%s%n", name);
        System.out.println("-".repeat(70));
        System.out.printf("  batch.size  = %d bytes (%d KB)%n", batchSize, batchSize / 1024);
        System.out.printf("  linger.ms   = %d ms%n", lingerMs);
        System.out.printf("  messages    = %d%n%n", messageCount);

        long startTime = System.currentTimeMillis();

        for (int i = 1; i <= messageCount; i++) {
            Transaction txn = Transaction.builder()
                    .fromAccount("ACC" + (i % 1000))
                    .toAccount("ACC" + ((i + 1) % 1000))
                    .amount(new BigDecimal("1000.00"))
                    .type("BULK_TRANSFER")
                    .build();

            ProducerRecord<String, Transaction> record =
                    new ProducerRecord<>(topic, txn.getFromAccount(), txn);

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    successCount.incrementAndGet();
                }
                latch.countDown();
            });
        }

        long sendTime = System.currentTimeMillis() - startTime;
        System.out.printf("  Send completed in: %d ms (non-blocking)%n", sendTime);

        latch.await();
        long totalTime = System.currentTimeMillis() - startTime;

        producer.close();

        long throughput = (messageCount * 1000L) / totalTime;

        System.out.printf("  Total time       : %d ms%n", totalTime);
        System.out.printf("  Throughput       : %,d msg/sec%n", throughput);
        System.out.printf("  Success          : %d / %d%n%n", successCount.get(), messageCount);

        return throughput;
    }

    private static void printComparison() {
        System.out.println("=".repeat(70));
        System.out.println("BATCHING ANALYSIS");
        System.out.println("=".repeat(70));

        System.out.println("\nHOW BATCHING WORKS:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  Producer Memory Layout:");
        System.out.println("  ┌────────────────────────────────────────────────────────────┐");
        System.out.println("  │                    buffer.memory (64 MB)                   │");
        System.out.println("  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐        │");
        System.out.println("  │  │ Partition 0  │ │ Partition 1  │ │ Partition 2  │  ...   │");
        System.out.println("  │  │ batch.size   │ │ batch.size   │ │ batch.size   │        │");
        System.out.println("  │  │   (32 KB)    │ │   (32 KB)    │ │   (32 KB)    │        │");
        System.out.println("  │  └──────────────┘ └──────────────┘ └──────────────┘        │");
        System.out.println("  └────────────────────────────────────────────────────────────┘");
        System.out.println("");

        System.out.println("\nBATCH SEND TRIGGERS:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  Trigger 1: batch.size reached");
        System.out.println("  ┌─────────────────────────────────────┐");
        System.out.println("  │ M1 │ M2 │ M3 │ ... │ Mn │ FULL!    │───► SEND");
        System.out.println("  └─────────────────────────────────────┘");
        System.out.println("                                32 KB");
        System.out.println("");
        System.out.println("  Trigger 2: linger.ms elapsed");
        System.out.println("  ┌─────────────────────┐");
        System.out.println("  │ M1 │ M2 │ M3 │      │───► SEND (20ms passed)");
        System.out.println("  └─────────────────────┘");
        System.out.println("         Partial batch");
        System.out.println("");

        System.out.println("\nSCENARIO COMPARISON:");
        System.out.println("-".repeat(70));
        System.out.println("| Scenario   | linger.ms | batch.size | Throughput   | Use Case        |");
        System.out.println("|------------|-----------|------------|--------------|-----------------|");
        System.out.println("| No Batch   | 0         | 16 KB      | ~3,000/s     | Low latency     |");
        System.out.println("| Moderate   | 20        | 32 KB      | ~8,000/s     | Balanced        |");
        System.out.println("| Aggressive | 100       | 64 KB      | ~15,000/s    | Max throughput  |");
        System.out.println("-".repeat(70));

        System.out.println("\nLATENCY vs THROUGHPUT:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  linger.ms=0      linger.ms=20     linger.ms=100");
        System.out.println("  ───────────      ────────────     ─────────────");
        System.out.println("  Latency: ●○○○○   Latency: ●●○○○   Latency: ●●●●○");
        System.out.println("  Thruput: ●●○○○   Thruput: ●●●●○   Thruput: ●●●●●");
        System.out.println("");
        System.out.println("  ● = Higher/More");
        System.out.println("");

        System.out.println("\nBANKING RECOMMENDATIONS:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  Real-time Payments (IMPS):");
        System.out.println("    linger.ms=0-5, batch.size=16KB");
        System.out.println("    → Minimal latency, immediate send");
        System.out.println("");
        System.out.println("  Bulk Transfers (Salary credits):");
        System.out.println("    linger.ms=50-100, batch.size=64KB");
        System.out.println("    → Maximum throughput");
        System.out.println("");
        System.out.println("  Audit Logging:");
        System.out.println("    linger.ms=20-50, batch.size=32KB");
        System.out.println("    → Balanced approach");
        System.out.println("");
        System.out.println("-".repeat(70));

        System.out.println("\nMEMORY CALCULATION:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  buffer.memory = batch.size × num_partitions × safety_factor");
        System.out.println("");
        System.out.println("  Example (6 partitions, 32KB batch):");
        System.out.println("    Minimum: 32KB × 6 = 192KB");
        System.out.println("    Safe:    32KB × 6 × 3 = 576KB");
        System.out.println("    Recommended: 64MB (handles spikes)");
        System.out.println("");
        System.out.println("  If buffer.memory exhausted:");
        System.out.println("    → max.block.ms timeout (default 60s)");
        System.out.println("    → Producer blocks or throws exception");
        System.out.println("");
        System.out.println("-".repeat(70));
    }
}