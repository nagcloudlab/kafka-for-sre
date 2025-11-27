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
 * DEMO 08: Compression
 *
 * WHAT CHANGED FROM DEMO 07:
 * +---------------------------+-----------+-----------+
 * | Config                    | Demo 07   | Demo 08   |
 * +---------------------------+-----------+-----------+
 * | compression.type          | none      | lz4       |
 * +---------------------------+-----------+-----------+
 *
 * COMPRESSION TYPES:
 * +-------------+-------------+-------------+-------------+------------------+
 * | Type        | Speed       | Ratio       | CPU         | Best For         |
 * +-------------+-------------+-------------+-------------+------------------+
 * | none        | Fastest     | 1:1         | Zero        | Low latency      |
 * | lz4         | Very Fast   | ~2-3x       | Low         | Balanced (Rec.)  |
 * | snappy      | Fast        | ~2-3x       | Low         | Google-style     |
 * | zstd        | Medium      | ~3-5x       | Medium      | Best ratio       |
 * | gzip        | Slow        | ~3-5x       | High        | Max compression  |
 * +-------------+-------------+-------------+-------------+------------------+
 *
 * WHERE COMPRESSION HAPPENS:
 *
 * ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
 * │   Producer   │    │    Broker    │    │   Consumer   │
 * │              │    │              │    │              │
 * │  Compress ───┼───►│ Store as-is ─┼───►│ Decompress   │
 * │  (batch)     │    │ (compressed) │    │  (batch)     │
 * └──────────────┘    └──────────────┘    └──────────────┘
 *
 * BENEFITS:
 * 1. Less network bandwidth (Producer → Broker)
 * 2. Less disk storage on broker
 * 3. Less network bandwidth (Broker → Consumer)
 * 4. Higher throughput (smaller batches = faster transfer)
 *
 * Banking Scenario: High-volume transaction logs, audit records
 */
public class Demo08_Compression {

    public static void main(String[] args) throws InterruptedException {

        System.out.println("=".repeat(70));
        System.out.println("DEMO 08: Compression");
        System.out.println("=".repeat(70));
        System.out.println("Comparing different compression algorithms\n");

        // Run scenarios with different compression types
        runScenario("none");
        runScenario("lz4");
        runScenario("snappy");
        runScenario("zstd");
        runScenario("gzip");

        printAnalysis();
    }

    private static void runScenario(String compressionType) throws InterruptedException {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

        // From Demo 06 & 07
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);

        // CHANGED: Compression
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);

        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(props);

        String topic = "fund-transfer-events";
        int messageCount = 10000;

        AtomicInteger successCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(messageCount);

        System.out.println("-".repeat(70));
        System.out.printf("Compression: %s%n", compressionType.toUpperCase());
        System.out.println("-".repeat(70));

        long startTime = System.currentTimeMillis();

        for (int i = 1; i <= messageCount; i++) {
            // Create realistic transaction with repetitive data (compresses well)
            Transaction txn = Transaction.builder()
                    .transactionId("TXN-2025-" + String.format("%08d", i))
                    .fromAccount("SAVINGS-ACC-" + String.format("%05d", i % 1000))
                    .toAccount("CURRENT-ACC-" + String.format("%05d", (i + 1) % 1000))
                    .amount(new BigDecimal("25000.00"))
                    .currency("INR")
                    .type("INTER_BANK_TRANSFER")
                    .status("INITIATED")
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

        latch.await();
        long duration = System.currentTimeMillis() - startTime;

        producer.close();

        long throughput = (messageCount * 1000L) / duration;

        System.out.printf("  Messages   : %,d%n", messageCount);
        System.out.printf("  Time       : %,d ms%n", duration);
        System.out.printf("  Throughput : %,d msg/sec%n", throughput);
        System.out.printf("  Success    : %,d%n%n", successCount.get());
    }

    private static void printAnalysis() {
        System.out.println("=".repeat(70));
        System.out.println("COMPRESSION ANALYSIS");
        System.out.println("=".repeat(70));

        System.out.println("\nCOMPRESSION COMPARISON:");
        System.out.println("-".repeat(70));
        System.out.println("| Type   | Compress | Decompress | Ratio | CPU   | Network |");
        System.out.println("|--------|----------|------------|-------|-------|---------|");
        System.out.println("| none   | -        | -          | 1.0x  | Zero  | High    |");
        System.out.println("| lz4    | ~500MB/s | ~1.5GB/s   | 2.5x  | Low   | Low     |");
        System.out.println("| snappy | ~400MB/s | ~1.0GB/s   | 2.3x  | Low   | Low     |");
        System.out.println("| zstd   | ~200MB/s | ~600MB/s   | 3.5x  | Med   | Lower   |");
        System.out.println("| gzip   | ~50MB/s  | ~200MB/s   | 3.8x  | High  | Lowest  |");
        System.out.println("-".repeat(70));

        System.out.println("\nHOW COMPRESSION WORKS:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  Without Compression:");
        System.out.println("  ┌─────────────────────────────────────┐");
        System.out.println("  │ M1 │ M2 │ M3 │ M4 │ M5 │  = 500 KB │───► Network ───► Broker");
        System.out.println("  └─────────────────────────────────────┘");
        System.out.println("");
        System.out.println("  With LZ4 Compression:");
        System.out.println("  ┌─────────────────────────────────────┐");
        System.out.println("  │ M1 │ M2 │ M3 │ M4 │ M5 │  = 500 KB │");
        System.out.println("  └─────────────────────────────────────┘");
        System.out.println("                    │");
        System.out.println("                    ▼ Compress");
        System.out.println("  ┌───────────────────┐");
        System.out.println("  │ Compressed = 200KB│───► Network ───► Broker");
        System.out.println("  └───────────────────┘");
        System.out.println("         60% less!");
        System.out.println("");

        System.out.println("\nCOMPRESSION FLOW:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ┌────────────┐      ┌────────────┐      ┌────────────┐");
        System.out.println("  │  PRODUCER  │      │   BROKER   │      │  CONSUMER  │");
        System.out.println("  │            │      │            │      │            │");
        System.out.println("  │ 1. Batch   │      │ 3. Store   │      │ 5. Receive │");
        System.out.println("  │    messages│      │    as-is   │      │    batch   │");
        System.out.println("  │            │      │            │      │            │");
        System.out.println("  │ 2. Compress│─────►│ (still     │─────►│ 6. Decomp- │");
        System.out.println("  │    batch   │      │ compressed)│      │    ress    │");
        System.out.println("  └────────────┘      └────────────┘      └────────────┘");
        System.out.println("");
        System.out.println("  Note: Broker does NOT decompress! Saves CPU on broker.");
        System.out.println("");

        System.out.println("\nWHEN DATA COMPRESSES WELL:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  HIGH Compression (3-5x):");
        System.out.println("  ✓ JSON with repetitive field names");
        System.out.println("  ✓ Text logs with common patterns");
        System.out.println("  ✓ Structured data with many similar records");
        System.out.println("  ✓ Banking transactions (similar format)");
        System.out.println("");
        System.out.println("  LOW Compression (1.1-1.5x):");
        System.out.println("  ✗ Already compressed data (images, videos)");
        System.out.println("  ✗ Encrypted data (appears random)");
        System.out.println("  ✗ Binary data with high entropy");
        System.out.println("");

        System.out.println("\nBANKING RECOMMENDATIONS:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ┌─────────────────────────────────────────────────────────────┐");
        System.out.println("  │ Use Case              │ Compression │ Reason               │");
        System.out.println("  ├───────────────────────┼─────────────┼──────────────────────┤");
        System.out.println("  │ Real-time payments    │ lz4         │ Fast, low latency    │");
        System.out.println("  │ Audit logs            │ zstd        │ Best ratio, archival │");
        System.out.println("  │ High-freq trading     │ none/lz4    │ Lowest latency       │");
        System.out.println("  │ Bulk settlements      │ zstd/gzip   │ Max compression      │");
        System.out.println("  │ Cross-region replicate│ zstd        │ Minimize WAN traffic │");
        System.out.println("  └─────────────────────────────────────────────────────────────┘");
        System.out.println("");

        System.out.println("\nPRODUCER vs BROKER COMPRESSION:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  Producer-side (compression.type in producer):");
        System.out.println("  ✓ Saves network bandwidth");
        System.out.println("  ✓ Reduces broker CPU");
        System.out.println("  ✓ Recommended approach");
        System.out.println("");
        System.out.println("  Broker-side (compression.type in topic config):");
        System.out.println("  • Broker may re-compress if different from producer");
        System.out.println("  • Use for enforcing compression policy");
        System.out.println("  • Set topic config: compression.type=producer (use producer's)");
        System.out.println("");
        System.out.println("  Command to set topic compression:");
        System.out.println("  kafka-configs.sh --bootstrap-server localhost:9092 \\");
        System.out.println("    --entity-type topics --entity-name fund-transfer-events \\");
        System.out.println("    --alter --add-config compression.type=lz4");
        System.out.println("");

        System.out.println("\nCONFIG COMPARISON:");
        System.out.println("-".repeat(70));
        System.out.println("| Config           | Demo07      | Demo08         |");
        System.out.println("|------------------|-------------|----------------|");
        System.out.println("| compression.type | none        | lz4            |");
        System.out.println("| batch.size       | 32 KB       | 32 KB          |");
        System.out.println("| linger.ms        | 20          | 20             |");
        System.out.println("| Network usage    | 100%        | ~40%           |");
        System.out.println("| CPU overhead     | 0%          | ~5%            |");
        System.out.println("-".repeat(70));

        System.out.println("\nSTORAGE IMPACT:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  10 Million transactions/day × 500 bytes avg = 5 GB/day");
        System.out.println("");
        System.out.println("  | Compression | Storage/Day | Storage/Month | Savings |");
        System.out.println("  |-------------|-------------|---------------|---------|");
        System.out.println("  | none        | 5.0 GB      | 150 GB        | -       |");
        System.out.println("  | lz4         | 2.0 GB      | 60 GB         | 60%     |");
        System.out.println("  | zstd        | 1.4 GB      | 42 GB         | 72%     |");
        System.out.println("  | gzip        | 1.3 GB      | 39 GB         | 74%     |");
        System.out.println("");
        System.out.println("-".repeat(70));
    }
}