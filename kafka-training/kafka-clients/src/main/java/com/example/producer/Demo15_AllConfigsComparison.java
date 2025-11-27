package com.example.producer;

import com.example.Transaction;
import com.example.JsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * DEMO 15: All Configs Comparison (Summary Benchmark)
 *
 * COMPREHENSIVE BENCHMARK:
 * Compare all producer configurations covered in Demos 01-14
 *
 * SCENARIOS TESTED:
 * 1. Fire-and-Forget (acks=0)
 * 2. Sync acks=1
 * 3. Sync acks=all
 * 4. Async with Callback
 * 5. Idempotent Producer
 * 6. Batching Optimized
 * 7. Compression (LZ4)
 * 8. Full Production Config
 *
 * METRICS COLLECTED:
 * - Throughput (msg/sec)
 * - Latency (avg, min, max)
 * - Success rate
 * - Memory efficiency
 */
public class Demo15_AllConfigsComparison {

    private static final String TOPIC = "fund-transfer-events";
    private static final int MESSAGE_COUNT = 10000;

    private static List<BenchmarkResult> results = new ArrayList<>();

    public static void main(String[] args) throws InterruptedException {

        System.out.println("=".repeat(70));
        System.out.println("DEMO 15: All Configs Comparison (Summary Benchmark)");
        System.out.println("=".repeat(70));
        System.out.println("Comprehensive benchmark of all producer configurations\n");

        System.out.printf("Test Parameters: %d messages per scenario%n", MESSAGE_COUNT);
        System.out.printf("Topic: %s%n%n", TOPIC);

        // Run all benchmarks
        runBenchmark("1. Fire-and-Forget (acks=0)", createFireAndForgetProps(), false);
        runBenchmark("2. Sync acks=1", createSyncAcks1Props(), true);
        runBenchmark("3. Sync acks=all", createSyncAcksAllProps(), true);
        runBenchmark("4. Async + Callback", createAsyncProps(), false);
        runBenchmark("5. Idempotent", createIdempotentProps(), false);
        runBenchmark("6. Batching Optimized", createBatchingProps(), false);
        runBenchmark("7. Compression (LZ4)", createCompressionProps(), false);
        runBenchmark("8. Production Config", createProductionProps(), false);

        // Print comparison table
        printComparisonTable();

        // Print recommendations
        printRecommendations();

        // Print all configs reference
        printAllConfigsReference();
    }

    private static void runBenchmark(String name, Properties props, boolean sync)
            throws InterruptedException {

        System.out.println("-".repeat(70));
        System.out.printf("Benchmark: %s%n", name);
        System.out.println("-".repeat(70));

        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(props);

        AtomicInteger success = new AtomicInteger(0);
        AtomicInteger failed = new AtomicInteger(0);
        AtomicLong totalLatency = new AtomicLong(0);
        AtomicLong minLatency = new AtomicLong(Long.MAX_VALUE);
        AtomicLong maxLatency = new AtomicLong(0);

        CountDownLatch latch = new CountDownLatch(MESSAGE_COUNT);

        long startTime = System.currentTimeMillis();

        for (int i = 1; i <= MESSAGE_COUNT; i++) {
            long sendStart = System.currentTimeMillis();

            Transaction txn = Transaction.builder()
                    .fromAccount("ACC" + String.format("%05d", i % 10000))
                    .toAccount("MERCHANT-001")
                    .amount(new BigDecimal("1000.00"))
                    .type("BENCHMARK")
                    .build();

            ProducerRecord<String, Transaction> record =
                    new ProducerRecord<>(TOPIC, txn.getFromAccount(), txn);

            if (sync) {
                // Synchronous send
                try {
                    Future<RecordMetadata> future = producer.send(record);
                    future.get();

                    long latency = System.currentTimeMillis() - sendStart;
                    success.incrementAndGet();
                    totalLatency.addAndGet(latency);
                    updateMinMax(minLatency, maxLatency, latency);

                } catch (Exception e) {
                    failed.incrementAndGet();
                }
                latch.countDown();
            } else {
                // Asynchronous send
                final long sendTime = sendStart;
                producer.send(record, (metadata, exception) -> {
                    long latency = System.currentTimeMillis() - sendTime;

                    if (exception == null) {
                        success.incrementAndGet();
                        totalLatency.addAndGet(latency);
                        updateMinMax(minLatency, maxLatency, latency);
                    } else {
                        failed.incrementAndGet();
                    }
                    latch.countDown();
                });
            }
        }

        long sendTime = System.currentTimeMillis() - startTime;
        latch.await();
        long totalTime = System.currentTimeMillis() - startTime;

        producer.close();

        // Calculate metrics
        int successCount = success.get();
        long throughput = (MESSAGE_COUNT * 1000L) / totalTime;
        double avgLatency = successCount > 0 ? totalLatency.get() / (double) successCount : 0;
        long min = minLatency.get() == Long.MAX_VALUE ? 0 : minLatency.get();
        long max = maxLatency.get();

        // Store result
        BenchmarkResult result = new BenchmarkResult(
                name, MESSAGE_COUNT, successCount, failed.get(),
                throughput, avgLatency, min, max, totalTime
        );
        results.add(result);

        // Print result
        System.out.printf("  Success/Failed : %d / %d%n", successCount, failed.get());
        System.out.printf("  Total Time     : %d ms%n", totalTime);
        System.out.printf("  Throughput     : %,d msg/sec%n", throughput);
        System.out.printf("  Latency (avg)  : %.2f ms%n", avgLatency);
        System.out.printf("  Latency (min)  : %d ms%n", min);
        System.out.printf("  Latency (max)  : %d ms%n%n", max);
    }

    private static void updateMinMax(AtomicLong min, AtomicLong max, long value) {
        min.updateAndGet(current -> Math.min(current, value));
        max.updateAndGet(current -> Math.max(current, value));
    }

    // ==================== PROPERTIES CONFIGURATIONS ====================

    private static Properties createBaseProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        return props;
    }

    private static Properties createFireAndForgetProps() {
        Properties props = createBaseProps();
        props.put(ProducerConfig.ACKS_CONFIG, "0");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        return props;
    }

    private static Properties createSyncAcks1Props() {
        Properties props = createBaseProps();
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        return props;
    }

    private static Properties createSyncAcksAllProps() {
        Properties props = createBaseProps();
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        return props;
    }

    private static Properties createAsyncProps() {
        Properties props = createBaseProps();
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        return props;
    }

    private static Properties createIdempotentProps() {
        Properties props = createBaseProps();
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        // acks=all, retries=MAX_INT auto-enforced
        return props;
    }

    private static Properties createBatchingProps() {
        Properties props = createBaseProps();
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);       // 64KB
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);           // Wait 20ms
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864); // 64MB
        return props;
    }

    private static Properties createCompressionProps() {
        Properties props = createBaseProps();
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        return props;
    }

    private static Properties createProductionProps() {
        Properties props = createBaseProps();

        // Durability
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // Reliability
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        // Performance
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 134217728); // 128MB
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        return props;
    }

    // ==================== RESULT PRINTING ====================

    private static void printComparisonTable() {
        System.out.println("=".repeat(70));
        System.out.println("BENCHMARK COMPARISON TABLE");
        System.out.println("=".repeat(70));
        System.out.println("");

        System.out.println("┌────────────────────────────┬───────────┬──────────┬──────────┬──────────┐");
        System.out.println("│ Configuration              │ Throughput│ Avg Lat  │ Max Lat  │ Success  │");
        System.out.println("│                            │ (msg/sec) │ (ms)     │ (ms)     │ Rate     │");
        System.out.println("├────────────────────────────┼───────────┼──────────┼──────────┼──────────┤");

        for (BenchmarkResult r : results) {
            String shortName = r.name.length() > 26 ? r.name.substring(0, 26) : r.name;
            double successRate = r.total > 0 ? (r.success * 100.0 / r.total) : 0;

            System.out.printf("│ %-26s │ %,9d │ %8.2f │ %8d │ %7.1f%% │%n",
                    shortName, r.throughput, r.avgLatency, r.maxLatency, successRate);
        }

        System.out.println("└────────────────────────────┴───────────┴──────────┴──────────┴──────────┘");
        System.out.println("");

        // Throughput ranking
        System.out.println("THROUGHPUT RANKING:");
        System.out.println("-".repeat(70));
        results.stream()
                .sorted((a, b) -> Long.compare(b.throughput, a.throughput))
                .forEach(r -> {
                    int bars = (int) (r.throughput / 500);
                    String bar = "█".repeat(Math.min(bars, 40));
                    System.out.printf("  %-28s %,8d msg/s │%s%n",
                            r.name.substring(0, Math.min(28, r.name.length())),
                            r.throughput, bar);
                });
        System.out.println("");

        // Latency ranking
        System.out.println("LATENCY RANKING (Lower is Better):");
        System.out.println("-".repeat(70));
        results.stream()
                .sorted((a, b) -> Double.compare(a.avgLatency, b.avgLatency))
                .forEach(r -> {
                    int bars = (int) (r.avgLatency * 5);
                    String bar = "░".repeat(Math.min(bars, 40));
                    System.out.printf("  %-28s %8.2f ms │%s%n",
                            r.name.substring(0, Math.min(28, r.name.length())),
                            r.avgLatency, bar);
                });
        System.out.println("");
    }

    private static void printRecommendations() {
        System.out.println("=".repeat(70));
        System.out.println("CONFIGURATION RECOMMENDATIONS");
        System.out.println("=".repeat(70));
        System.out.println("");

        System.out.println("┌─────────────────────────────────────────────────────────────────────┐");
        System.out.println("│ USE CASE                        │ RECOMMENDED CONFIG               │");
        System.out.println("├─────────────────────────────────┼──────────────────────────────────┤");
        System.out.println("│ Real-time payments (low latency)│ Async + acks=all + Idempotent    │");
        System.out.println("│ High-value transfers (RTGS)     │ Sync + acks=all + Idempotent     │");
        System.out.println("│ Bulk processing (salary credits)│ Batching + Compression           │");
        System.out.println("│ Audit logging                   │ Production Config                │");
        System.out.println("│ Analytics/Metrics (loss OK)     │ Fire-and-Forget                  │");
        System.out.println("│ Cross-account atomicity         │ Transactional Producer           │");
        System.out.println("└─────────────────────────────────┴──────────────────────────────────┘");
        System.out.println("");

        System.out.println("DELIVERY SEMANTICS:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ┌─────────────────┬──────────────────────────────────────────────┐");
        System.out.println("  │ Semantic        │ Configuration                                │");
        System.out.println("  ├─────────────────┼──────────────────────────────────────────────┤");
        System.out.println("  │ At-most-once    │ acks=0, retries=0                            │");
        System.out.println("  │ At-least-once   │ acks=all, retries>0                          │");
        System.out.println("  │ Exactly-once    │ enable.idempotence=true OR transactional.id  │");
        System.out.println("  └─────────────────┴──────────────────────────────────────────────┘");
        System.out.println("");

        System.out.println("PERFORMANCE TUNING PRIORITIES:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  FOR THROUGHPUT:");
        System.out.println("  1. Increase batch.size (32KB → 64KB → 128KB)");
        System.out.println("  2. Increase linger.ms (0 → 10 → 50)");
        System.out.println("  3. Enable compression (lz4 recommended)");
        System.out.println("  4. Use async sends with callbacks");
        System.out.println("  5. Increase buffer.memory for spikes");
        System.out.println("");
        System.out.println("  FOR LATENCY:");
        System.out.println("  1. Set linger.ms=0 (immediate send)");
        System.out.println("  2. Smaller batch.size (if messages sparse)");
        System.out.println("  3. acks=1 (if some loss acceptable)");
        System.out.println("  4. Avoid compression (CPU overhead)");
        System.out.println("");
        System.out.println("  FOR DURABILITY:");
        System.out.println("  1. acks=all (required)");
        System.out.println("  2. enable.idempotence=true (prevent duplicates)");
        System.out.println("  3. min.insync.replicas=2 (topic config)");
        System.out.println("  4. retries=MAX_INT (never give up)");
        System.out.println("  5. Use transactions for cross-topic atomicity");
        System.out.println("");
    }

    private static void printAllConfigsReference() {
        System.out.println("=".repeat(70));
        System.out.println("ALL PRODUCER CONFIGS REFERENCE");
        System.out.println("=".repeat(70));
        System.out.println("");

        System.out.println("CORE CONFIGS:");
        System.out.println("-".repeat(70));
        System.out.println("┌──────────────────────────────┬─────────────┬───────────────────────┐");
        System.out.println("│ Config                       │ Default     │ Description           │");
        System.out.println("├──────────────────────────────┼─────────────┼───────────────────────┤");
        System.out.println("│ bootstrap.servers            │ (required)  │ Kafka broker list     │");
        System.out.println("│ key.serializer               │ (required)  │ Key serializer class  │");
        System.out.println("│ value.serializer             │ (required)  │ Value serializer class│");
        System.out.println("│ client.id                    │ \"\"          │ Producer identifier   │");
        System.out.println("└──────────────────────────────┴─────────────┴───────────────────────┘");
        System.out.println("");

        System.out.println("DELIVERY CONFIGS:");
        System.out.println("-".repeat(70));
        System.out.println("┌──────────────────────────────┬─────────────┬───────────────────────┐");
        System.out.println("│ Config                       │ Default     │ Values                │");
        System.out.println("├──────────────────────────────┼─────────────┼───────────────────────┤");
        System.out.println("│ acks                         │ all         │ 0, 1, all             │");
        System.out.println("│ retries                      │ MAX_INT     │ 0 to MAX_INT          │");
        System.out.println("│ retry.backoff.ms             │ 100         │ Backoff between retry │");
        System.out.println("│ retry.backoff.max.ms         │ 1000        │ Max backoff           │");
        System.out.println("│ delivery.timeout.ms          │ 120000      │ Total delivery time   │");
        System.out.println("│ request.timeout.ms           │ 30000       │ Single request timeout│");
        System.out.println("│ enable.idempotence           │ true        │ Prevent duplicates    │");
        System.out.println("│ transactional.id             │ null        │ Transaction ID        │");
        System.out.println("└──────────────────────────────┴─────────────┴───────────────────────┘");
        System.out.println("");

        System.out.println("BATCHING CONFIGS:");
        System.out.println("-".repeat(70));
        System.out.println("┌──────────────────────────────┬─────────────┬───────────────────────┐");
        System.out.println("│ Config                       │ Default     │ Recommendation        │");
        System.out.println("├──────────────────────────────┼─────────────┼───────────────────────┤");
        System.out.println("│ batch.size                   │ 16384       │ 32KB-64KB for thruput │");
        System.out.println("│ linger.ms                    │ 0           │ 5-50ms for batching   │");
        System.out.println("│ buffer.memory                │ 33554432    │ 64MB-256MB for spikes │");
        System.out.println("│ max.block.ms                 │ 60000       │ Backpressure timeout  │");
        System.out.println("└──────────────────────────────┴─────────────┴───────────────────────┘");
        System.out.println("");

        System.out.println("COMPRESSION CONFIGS:");
        System.out.println("-".repeat(70));
        System.out.println("┌──────────────────────────────┬─────────────┬───────────────────────┐");
        System.out.println("│ Config                       │ Default     │ Options               │");
        System.out.println("├──────────────────────────────┼─────────────┼───────────────────────┤");
        System.out.println("│ compression.type             │ none        │ none,gzip,snappy,lz4, │");
        System.out.println("│                              │             │ zstd                  │");
        System.out.println("└──────────────────────────────┴─────────────┴───────────────────────┘");
        System.out.println("");

        System.out.println("PARTITIONING CONFIGS:");
        System.out.println("-".repeat(70));
        System.out.println("┌──────────────────────────────┬─────────────┬───────────────────────┐");
        System.out.println("│ Config                       │ Default     │ Description           │");
        System.out.println("├──────────────────────────────┼─────────────┼───────────────────────┤");
        System.out.println("│ partitioner.class            │ Default     │ Custom partitioner    │");
        System.out.println("│ max.in.flight.requests       │ 5           │ Pipeline depth        │");
        System.out.println("└──────────────────────────────┴─────────────┴───────────────────────┘");
        System.out.println("");

        System.out.println("INTERCEPTOR CONFIG:");
        System.out.println("-".repeat(70));
        System.out.println("┌──────────────────────────────┬─────────────┬───────────────────────┐");
        System.out.println("│ Config                       │ Default     │ Description           │");
        System.out.println("├──────────────────────────────┼─────────────┼───────────────────────┤");
        System.out.println("│ interceptor.classes          │ \"\"          │ Comma-separated list  │");
        System.out.println("└──────────────────────────────┴─────────────┴───────────────────────┘");
        System.out.println("");

        System.out.println("=".repeat(70));
        System.out.println("PRODUCTION-READY TEMPLATE:");
        System.out.println("=".repeat(70));
        System.out.println("");
        System.out.println("  Properties props = new Properties();");
        System.out.println("");
        System.out.println("  // Connection");
        System.out.println("  props.put(\"bootstrap.servers\", \"broker1:9092,broker2:9092,broker3:9092\");");
        System.out.println("  props.put(\"client.id\", \"payment-producer-1\");");
        System.out.println("");
        System.out.println("  // Serializers");
        System.out.println("  props.put(\"key.serializer\", StringSerializer.class.getName());");
        System.out.println("  props.put(\"value.serializer\", JsonSerializer.class.getName());");
        System.out.println("");
        System.out.println("  // Durability (CRITICAL for banking)");
        System.out.println("  props.put(\"acks\", \"all\");");
        System.out.println("  props.put(\"enable.idempotence\", true);");
        System.out.println("  props.put(\"retries\", Integer.MAX_VALUE);");
        System.out.println("  props.put(\"max.in.flight.requests.per.connection\", 5);");
        System.out.println("");
        System.out.println("  // Timeouts");
        System.out.println("  props.put(\"delivery.timeout.ms\", 120000);");
        System.out.println("  props.put(\"request.timeout.ms\", 30000);");
        System.out.println("  props.put(\"retry.backoff.ms\", 100);");
        System.out.println("");
        System.out.println("  // Batching & Performance");
        System.out.println("  props.put(\"batch.size\", 65536);        // 64KB");
        System.out.println("  props.put(\"linger.ms\", 10);");
        System.out.println("  props.put(\"buffer.memory\", 134217728); // 128MB");
        System.out.println("  props.put(\"compression.type\", \"lz4\");");
        System.out.println("");
        System.out.println("  // For exactly-once across topics (optional)");
        System.out.println("  // props.put(\"transactional.id\", \"transfer-processor-1\");");
        System.out.println("");

        System.out.println("=".repeat(70));
        System.out.println("DEMO SUMMARY - Day 1 Producer Concepts Covered:");
        System.out.println("=".repeat(70));
        System.out.println("");
        System.out.println("  Demo 01: Fire-and-Forget         → acks=0, maximum throughput");
        System.out.println("  Demo 02: Sync acks=1             → Leader acknowledgment");
        System.out.println("  Demo 03: Sync acks=all           → Full durability");
        System.out.println("  Demo 04: Async Callback          → Non-blocking with confirmation");
        System.out.println("  Demo 05: Retries & Errors        → Resilience configuration");
        System.out.println("  Demo 06: Idempotent Producer     → Duplicate prevention");
        System.out.println("  Demo 07: Batching                → Throughput optimization");
        System.out.println("  Demo 08: Compression             → Network & storage efficiency");
        System.out.println("  Demo 09: Key-based Partitioning  → Message ordering");
        System.out.println("  Demo 10: Custom Partitioner      → Business-logic routing");
        System.out.println("  Demo 11: Headers & Metadata      → Tracing & audit");
        System.out.println("  Demo 12: Interceptors            → Cross-cutting concerns");
        System.out.println("  Demo 13: Transactional Producer  → Exactly-once semantics");
        System.out.println("  Demo 14: Buffer & Backpressure   → Stability under load");
        System.out.println("  Demo 15: All Configs Comparison  → Summary benchmark");
        System.out.println("");
        System.out.println("=".repeat(70));
    }

    // ==================== RESULT CLASS ====================

    static class BenchmarkResult {
        String name;
        int total;
        int success;
        int failed;
        long throughput;
        double avgLatency;
        long minLatency;
        long maxLatency;
        long totalTime;

        BenchmarkResult(String name, int total, int success, int failed,
                        long throughput, double avgLatency, long minLatency,
                        long maxLatency, long totalTime) {
            this.name = name;
            this.total = total;
            this.success = success;
            this.failed = failed;
            this.throughput = throughput;
            this.avgLatency = avgLatency;
            this.minLatency = minLatency;
            this.maxLatency = maxLatency;
            this.totalTime = totalTime;
        }
    }
}