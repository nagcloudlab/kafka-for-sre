package com.example.producer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Metrics Interceptor: Collect producer metrics for monitoring
 *
 * METRICS COLLECTED:
 * - Total messages sent
 * - Messages per topic
 * - Success/failure counts
 * - Latency statistics
 *
 * USE CASE:
 * - Prometheus/Grafana integration
 * - Custom dashboards
 * - Alerting on failures
 */
public class MetricsInterceptor implements ProducerInterceptor<String, Object> {

    private final AtomicLong totalSent = new AtomicLong(0);
    private final AtomicLong totalSuccess = new AtomicLong(0);
    private final AtomicLong totalFailed = new AtomicLong(0);
    private final AtomicLong totalLatency = new AtomicLong(0);
    private final ConcurrentHashMap<String, AtomicLong> topicCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Long> sendTimestamps = new ConcurrentHashMap<>();

    private static MetricsInterceptor instance;

    @Override
    public void configure(Map<String, ?> configs) {
        instance = this;
        System.out.println("  [MetricsInterceptor] Initialized");
    }

    @Override
    public ProducerRecord<String, Object> onSend(ProducerRecord<String, Object> record) {
        long count = totalSent.incrementAndGet();

        // Track per-topic count
        topicCounts.computeIfAbsent(record.topic(), k -> new AtomicLong(0)).incrementAndGet();

        // Store send timestamp for latency calculation
        sendTimestamps.put(count, System.currentTimeMillis());

        // Attach sequence number for correlation
        record.headers().add("metrics-seq", String.valueOf(count).getBytes());

        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            totalSuccess.incrementAndGet();

            // Calculate latency (approximate)
            long now = System.currentTimeMillis();
            long sent = sendTimestamps.values().stream()
                    .filter(t -> t <= now)
                    .findFirst()
                    .orElse(now);
            totalLatency.addAndGet(now - sent);
        } else {
            totalFailed.incrementAndGet();
        }
    }

    @Override
    public void close() {
        // Final metrics report
        System.out.println("\n  [MetricsInterceptor] Final Report:");
        System.out.printf("    Total Sent     : %d%n", totalSent.get());
        System.out.printf("    Total Success  : %d%n", totalSuccess.get());
        System.out.printf("    Total Failed   : %d%n", totalFailed.get());
        System.out.printf("    Success Rate   : %.2f%%%n",
                totalSent.get() > 0 ? (totalSuccess.get() * 100.0 / totalSent.get()) : 0);
        System.out.printf("    Avg Latency    : %.2f ms%n",
                totalSuccess.get() > 0 ? (totalLatency.get() * 1.0 / totalSuccess.get()) : 0);
        System.out.println("    Per-Topic Counts:");
        topicCounts.forEach((topic, count) ->
                System.out.printf("      %s: %d%n", topic, count.get()));
    }

    // Static access for external monitoring systems
    public static MetricsInterceptor getInstance() {
        return instance;
    }

    public long getTotalSent() { return totalSent.get(); }
    public long getTotalSuccess() { return totalSuccess.get(); }
    public long getTotalFailed() { return totalFailed.get(); }
}