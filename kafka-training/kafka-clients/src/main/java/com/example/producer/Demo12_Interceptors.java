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

/**
 * DEMO 12: Interceptors
 *
 * WHAT CHANGED FROM DEMO 11:
 * +---------------------------+-------------------+---------------------+
 * | Aspect                    | Demo 11           | Demo 12             |
 * +---------------------------+-------------------+---------------------+
 * | Cross-cutting concerns    | Manual headers    | Automatic via       |
 * |                           |                   | interceptors        |
 * | Metrics collection        | None              | MetricsInterceptor  |
 * | Logging                   | Manual            | LoggingInterceptor  |
 * | Audit headers             | Manual per msg    | Auto via AuditInt.  |
 * +---------------------------+-------------------+---------------------+
 *
 * INTERCEPTOR INTERFACE:
 *
 * public interface ProducerInterceptor<K, V> {
 *     void configure(Map<String, ?> configs);
 *     ProducerRecord<K, V> onSend(ProducerRecord<K, V> record);
 *     void onAcknowledgement(RecordMetadata metadata, Exception exception);
 *     void close();
 * }
 *
 * INTERCEPTOR CHAIN:
 *
 * Producer.send()
 *     → Interceptor1.onSend()
 *     → Interceptor2.onSend()
 *     → Serializer
 *     → Partitioner
 *     → Broker
 *     → Interceptor2.onAcknowledgement()
 *     → Interceptor1.onAcknowledgement()
 *     → Callback
 *
 * USE CASES:
 * 1. Logging/Debugging
 * 2. Metrics collection
 * 3. Automatic header injection
 * 4. Message transformation
 * 5. Security/Encryption
 * 6. Audit trail
 *
 * Banking Scenario: Compliance audit, monitoring, centralized logging
 */
public class Demo12_Interceptors {

    public static void main(String[] args) throws InterruptedException {

        System.out.println("=".repeat(70));
        System.out.println("DEMO 12: Interceptors");
        System.out.println("=".repeat(70));
        System.out.println("Cross-cutting concerns: logging, metrics, audit\n");

        // Part 1: Single interceptor
        runSingleInterceptorDemo();

        // Part 2: Chained interceptors
        runChainedInterceptorsDemo();

        printAnalysis();
    }

    private static void runSingleInterceptorDemo() throws InterruptedException {
        System.out.println("-".repeat(70));
        System.out.println("PART 1: Single Interceptor (Logging)");
        System.out.println("-".repeat(70));
        System.out.println("");

        Properties props = createBaseProperties();

        // CHANGED: Add single interceptor
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                LoggingInterceptor.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "logging-demo-producer");

        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(props);

        String topic = "fund-transfer-events";
        CountDownLatch latch = new CountDownLatch(3);

        System.out.println("\nSending 3 messages with LoggingInterceptor:\n");

        for (int i = 1; i <= 3; i++) {
            Transaction txn = Transaction.builder()
                    .fromAccount("ACC00" + i)
                    .toAccount("ACC00" + (i + 1))
                    .amount(new BigDecimal(i * 10000))
                    .type("TRANSFER")
                    .build();

            ProducerRecord<String, Transaction> record =
                    new ProducerRecord<>(topic, txn.getFromAccount(), txn);

            producer.send(record, (metadata, exception) -> latch.countDown());
            Thread.sleep(100);
        }

        latch.await();
        producer.close();
        System.out.println();
    }

    private static void runChainedInterceptorsDemo() throws InterruptedException {
        System.out.println("-".repeat(70));
        System.out.println("PART 2: Chained Interceptors (Audit → Metrics → Logging)");
        System.out.println("-".repeat(70));
        System.out.println("");

        Properties props = createBaseProperties();

        // CHANGED: Chain multiple interceptors (order matters!)
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                String.join(",",
                        AuditInterceptor.class.getName(),
                        MetricsInterceptor.class.getName(),
                        LoggingInterceptor.class.getName()
                ));
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "chained-interceptor-producer");

        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(props);

        String topic = "fund-transfer-events";
        int messageCount = 10;
        CountDownLatch latch = new CountDownLatch(messageCount);

        System.out.println("\nSending " + messageCount + " messages with chained interceptors:\n");

        for (int i = 1; i <= messageCount; i++) {
            Transaction txn = Transaction.builder()
                    .fromAccount("ACC" + String.format("%03d", i))
                    .toAccount("MERCHANT-001")
                    .amount(new BigDecimal(i * 5000))
                    .type(i % 2 == 0 ? "PAYMENT" : "TRANSFER")
                    .build();

            ProducerRecord<String, Transaction> record =
                    new ProducerRecord<>(topic, txn.getFromAccount(), txn);

            final int msgNum = i;
            producer.send(record, (metadata, exception) -> {
                if (msgNum <= 2 && exception == null) {
                    System.out.printf("  [CALLBACK] Message %d delivered%n", msgNum);
                }
                latch.countDown();
            });

            Thread.sleep(50);
        }

        latch.await();
        System.out.println("\nClosing producer (triggers interceptor cleanup):\n");
        producer.close();
        System.out.println();
    }

    private static Properties createBaseProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        return props;
    }

    private static void printAnalysis() {
        System.out.println("=".repeat(70));
        System.out.println("INTERCEPTOR ANALYSIS");
        System.out.println("=".repeat(70));

        System.out.println("\nINTERCEPTOR INTERFACE:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  public interface ProducerInterceptor<K, V> {");
        System.out.println("");
        System.out.println("      // Called once during producer initialization");
        System.out.println("      void configure(Map<String, ?> configs);");
        System.out.println("");
        System.out.println("      // Called BEFORE serialization - can modify record");
        System.out.println("      ProducerRecord<K, V> onSend(ProducerRecord<K, V> record);");
        System.out.println("");
        System.out.println("      // Called AFTER broker acknowledgment (or failure)");
        System.out.println("      void onAcknowledgement(RecordMetadata metadata, Exception ex);");
        System.out.println("");
        System.out.println("      // Called during producer.close()");
        System.out.println("      void close();");
        System.out.println("  }");
        System.out.println("");

        System.out.println("\nINTERCEPTOR CHAIN FLOW:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │                      producer.send(record)                      │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("                               │");
        System.out.println("                               ▼");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │  Interceptor 1 (Audit)     │  onSend() - Add audit headers      │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("                               │");
        System.out.println("                               ▼");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │  Interceptor 2 (Metrics)   │  onSend() - Track send count       │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("                               │");
        System.out.println("                               ▼");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │  Interceptor 3 (Logging)   │  onSend() - Log message details    │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("                               │");
        System.out.println("                               ▼");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │  Serializer → Partitioner → Network → Broker                   │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("                               │");
        System.out.println("                               ▼");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │  Interceptor 3 (Logging)   │  onAcknowledgement() - Log result  │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("                               │");
        System.out.println("                               ▼");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │  Interceptor 2 (Metrics)   │  onAcknowledgement() - Track ack   │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("                               │");
        System.out.println("                               ▼");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │  Interceptor 1 (Audit)     │  onAcknowledgement() - Audit log   │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("                               │");
        System.out.println("                               ▼");
        System.out.println("  ┌─────────────────────────────────────────────────────────────────┐");
        System.out.println("  │                         Callback                                │");
        System.out.println("  └─────────────────────────────────────────────────────────────────┘");
        System.out.println("");
        System.out.println("  Note: onSend() runs in ORDER, onAcknowledgement() runs in REVERSE");
        System.out.println("");

        System.out.println("\nINTERCEPTOR USE CASES:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ┌─────────────────────┬──────────────────────────────────────────┐");
        System.out.println("  │ Interceptor Type    │ Purpose                                  │");
        System.out.println("  ├─────────────────────┼──────────────────────────────────────────┤");
        System.out.println("  │ Logging             │ Debug, trace message flow                │");
        System.out.println("  │ Metrics             │ Prometheus/Grafana, custom monitoring    │");
        System.out.println("  │ Audit               │ Compliance, automatic audit headers      │");
        System.out.println("  │ Tracing             │ Distributed tracing (OpenTelemetry)      │");
        System.out.println("  │ Encryption          │ Encrypt payload before send              │");
        System.out.println("  │ Validation          │ Validate messages before send            │");
        System.out.println("  │ Transformation      │ Modify/enrich messages                   │");
        System.out.println("  │ Rate Limiting       │ Throttle producer rate                   │");
        System.out.println("  └─────────────────────┴──────────────────────────────────────────┘");
        System.out.println("");

        System.out.println("\nBANKING INTERCEPTOR EXAMPLES:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  1. COMPLIANCE INTERCEPTOR:");
        System.out.println("     - Add regulatory headers (AML, CTR flags)");
        System.out.println("     - Log high-value transactions");
        System.out.println("     - Track cross-border transfers");
        System.out.println("");
        System.out.println("  2. SECURITY INTERCEPTOR:");
        System.out.println("     - Encrypt sensitive fields (PII, card numbers)");
        System.out.println("     - Add checksum/signature headers");
        System.out.println("     - Validate message integrity");
        System.out.println("");
        System.out.println("  3. MONITORING INTERCEPTOR:");
        System.out.println("     - Track transactions per second");
        System.out.println("     - Monitor latency percentiles");
        System.out.println("     - Alert on failure rates");
        System.out.println("");

        System.out.println("\nCONFIGURATION:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  // Single interceptor");
        System.out.println("  props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,");
        System.out.println("            \"com.example.producer.LoggingInterceptor\");");
        System.out.println("");
        System.out.println("  // Multiple interceptors (comma-separated, order matters!)");
        System.out.println("  props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,");
        System.out.println("            \"com.example.producer.AuditInterceptor,\" +");
        System.out.println("            \"com.example.producer.MetricsInterceptor,\" +");
        System.out.println("            \"com.example.producer.LoggingInterceptor\");");
        System.out.println("");
        System.out.println("  // Pass custom config to interceptor");
        System.out.println("  props.put(\"my.interceptor.custom.setting\", \"value\");");
        System.out.println("");

        System.out.println("\nINTERCEPTOR BEST PRACTICES:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ✓ DO:");
        System.out.println("    • Keep onSend() fast (blocks producer)");
        System.out.println("    • Handle exceptions gracefully (don't break producer)");
        System.out.println("    • Use thread-safe data structures");
        System.out.println("    • Clean up resources in close()");
        System.out.println("    • Order interceptors correctly (audit before logging)");
        System.out.println("");
        System.out.println("  ✗ DON'T:");
        System.out.println("    • Make blocking calls in onSend()");
        System.out.println("    • Throw exceptions (log and continue)");
        System.out.println("    • Modify record key (breaks partitioning)");
        System.out.println("    • Store large state in memory");
        System.out.println("    • Forget to handle null metadata in onAcknowledgement()");
        System.out.println("");

        System.out.println("\nINTERCEPTOR vs CALLBACK vs HEADERS:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ┌─────────────────┬─────────────────────────────────────────────┐");
        System.out.println("  │ Approach        │ When to Use                                 │");
        System.out.println("  ├─────────────────┼─────────────────────────────────────────────┤");
        System.out.println("  │ Interceptor     │ Cross-cutting concerns for ALL messages     │");
        System.out.println("  │                 │ Centralized policy (audit, metrics, logging)│");
        System.out.println("  ├─────────────────┼─────────────────────────────────────────────┤");
        System.out.println("  │ Callback        │ Per-message result handling                 │");
        System.out.println("  │                 │ Business logic on send completion           │");
        System.out.println("  ├─────────────────┼─────────────────────────────────────────────┤");
        System.out.println("  │ Headers         │ Per-message metadata                        │");
        System.out.println("  │                 │ Variable data (correlation-id, priority)    │");
        System.out.println("  └─────────────────┴─────────────────────────────────────────────┘");
        System.out.println("");

        System.out.println("\nCONFIG COMPARISON:");
        System.out.println("-".repeat(70));
        System.out.println("| Aspect              | Demo11 (Headers) | Demo12 (Interceptors) |");
        System.out.println("|---------------------|------------------|----------------------|");
        System.out.println("| Audit headers       | Manual per msg   | Automatic (central)  |");
        System.out.println("| Metrics             | None             | Built-in collection  |");
        System.out.println("| Logging             | Manual           | Automatic            |");
        System.out.println("| Maintainability     | Scattered        | Centralized          |");
        System.out.println("| Flexibility         | Per-message      | Global policy        |");
        System.out.println("-".repeat(70));
    }
}