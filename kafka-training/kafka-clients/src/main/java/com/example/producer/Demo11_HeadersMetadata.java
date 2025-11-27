package com.example.producer;

import com.example.Transaction;
import com.example.JsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

/**
 * DEMO 11: Headers & Metadata
 *
 * WHAT CHANGED FROM DEMO 10:
 * +---------------------------+-------------------+---------------------+
 * | Aspect                    | Demo 10           | Demo 11             |
 * +---------------------------+-------------------+---------------------+
 * | Message metadata          | Key only          | Key + Headers       |
 * | Tracing                   | None              | Correlation ID      |
 * | Routing info              | In key            | In headers          |
 * | Audit data                | In payload        | Separated to headers|
 * +---------------------------+-------------------+---------------------+
 *
 * WHAT ARE HEADERS:
 * - Key-value pairs attached to each message
 * - Not part of message payload
 * - Preserved through the entire pipeline (Producer → Broker → Consumer)
 * - Multiple headers with same key allowed
 *
 * HEADER STRUCTURE:
 * ┌─────────────────────────────────────────────────────────────┐
 * │                     Kafka Message                           │
 * ├─────────────────────────────────────────────────────────────┤
 * │ Key:     ACC001                                             │
 * │ Value:   {"transactionId": "TXN-001", "amount": 50000, ...} │
 * │ Headers: [                                                  │
 * │   {"correlation-id": "REQ-12345"},                          │
 * │   {"source-system": "CORE-BANKING"},                        │
 * │   {"timestamp": "2025-11-24T10:30:00Z"},                    │
 * │   {"priority": "HIGH"}                                      │
 * │ ]                                                           │
 * └─────────────────────────────────────────────────────────────┘
 *
 * USE CASES:
 * 1. Distributed tracing (correlation ID, trace ID, span ID)
 * 2. Message routing (without parsing payload)
 * 3. Audit metadata (source system, user, timestamp)
 * 4. Content type (JSON, Avro, Protobuf)
 * 5. Schema version (for evolution)
 * 6. Security context (tenant ID, auth token hash)
 *
 * Banking Scenario: Transaction tracing, audit trails, multi-tenant routing
 */
public class Demo11_HeadersMetadata {

    public static void main(String[] args) throws InterruptedException {

        System.out.println("=".repeat(70));
        System.out.println("DEMO 11: Headers & Metadata");
        System.out.println("=".repeat(70));
        System.out.println("Adding metadata to messages for tracing and auditing\n");

        // Part 1: Basic headers
        runBasicHeadersDemo();

        // Part 2: Distributed tracing headers
        runTracingHeadersDemo();

        // Part 3: Banking audit headers
        runAuditHeadersDemo();

        printAnalysis();
    }

    private static void runBasicHeadersDemo() throws InterruptedException {
        Properties props = createBaseProperties();
        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(props);

        String topic = "fund-transfer-events";

        System.out.println("-".repeat(70));
        System.out.println("PART 1: Basic Headers");
        System.out.println("-".repeat(70));
        System.out.println("Adding simple key-value metadata to messages\n");

        CountDownLatch latch = new CountDownLatch(3);

        for (int i = 1; i <= 3; i++) {
            Transaction txn = Transaction.builder()
                    .fromAccount("ACC00" + i)
                    .toAccount("ACC00" + (i + 1))
                    .amount(new BigDecimal(i * 10000))
                    .type("TRANSFER")
                    .build();

            // Create headers
            List<Header> headers = new ArrayList<>();
            headers.add(new RecordHeader("content-type", "application/json".getBytes(StandardCharsets.UTF_8)));
            headers.add(new RecordHeader("version", "1.0".getBytes(StandardCharsets.UTF_8)));
            headers.add(new RecordHeader("priority", (i == 1 ? "HIGH" : "NORMAL").getBytes(StandardCharsets.UTF_8)));

            // ProducerRecord with headers
            ProducerRecord<String, Transaction> record = new ProducerRecord<>(
                    topic,                          // topic
                    null,                           // partition (null = auto)
                    System.currentTimeMillis(),     // timestamp
                    txn.getFromAccount(),           // key
                    txn,                            // value
                    headers                         // headers
            );

            final int msgNum = i;
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.printf("  Message %d:%n", msgNum);
                    System.out.printf("    Key: %s%n", record.key());
                    System.out.printf("    Partition: %d, Offset: %d%n", metadata.partition(), metadata.offset());
                    System.out.print("    Headers: ");
                    record.headers().forEach(h ->
                            System.out.printf("[%s=%s] ", h.key(), new String(h.value(), StandardCharsets.UTF_8)));
                    System.out.println("\n");
                }
                latch.countDown();
            });

            Thread.sleep(50);
        }

        latch.await();
        producer.close();
    }

    private static void runTracingHeadersDemo() throws InterruptedException {
        Properties props = createBaseProperties();
        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(props);

        String topic = "fund-transfer-events";

        System.out.println("-".repeat(70));
        System.out.println("PART 2: Distributed Tracing Headers");
        System.out.println("-".repeat(70));
        System.out.println("OpenTelemetry-style tracing context propagation\n");

        // Simulate a request flowing through multiple services
        String traceId = UUID.randomUUID().toString().replace("-", "").substring(0, 32);
        String parentSpanId = generateSpanId();

        System.out.printf("  Trace ID: %s%n", traceId);
        System.out.printf("  Parent Span: %s (API Gateway)%n%n", parentSpanId);

        CountDownLatch latch = new CountDownLatch(3);

        String[] services = {"payment-service", "fraud-check", "notification-service"};

        for (int i = 0; i < services.length; i++) {
            String currentSpanId = generateSpanId();
            String service = services[i];

            Transaction txn = Transaction.builder()
                    .fromAccount("ACC001")
                    .toAccount("ACC002")
                    .amount(new BigDecimal("50000.00"))
                    .type("TRACED_TRANSFER")
                    .build();

            // OpenTelemetry-compatible headers
            List<Header> headers = new ArrayList<>();
            headers.add(new RecordHeader("traceparent",
                    String.format("00-%s-%s-01", traceId, currentSpanId).getBytes(StandardCharsets.UTF_8)));
            headers.add(new RecordHeader("trace-id", traceId.getBytes(StandardCharsets.UTF_8)));
            headers.add(new RecordHeader("span-id", currentSpanId.getBytes(StandardCharsets.UTF_8)));
            headers.add(new RecordHeader("parent-span-id", parentSpanId.getBytes(StandardCharsets.UTF_8)));
            headers.add(new RecordHeader("source-service", service.getBytes(StandardCharsets.UTF_8)));
            headers.add(new RecordHeader("timestamp", Instant.now().toString().getBytes(StandardCharsets.UTF_8)));

            ProducerRecord<String, Transaction> record = new ProducerRecord<>(
                    topic, null, System.currentTimeMillis(),
                    txn.getFromAccount(), txn, headers
            );

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.printf("  [%s]%n", service);
                    System.out.printf("    Span ID: %s%n", currentSpanId);
                    System.out.printf("    Partition: %d, Offset: %d%n", metadata.partition(), metadata.offset());
                }
                latch.countDown();
            });

            // Update parent for next service
            parentSpanId = currentSpanId;
            Thread.sleep(50);
        }

        latch.await();
        producer.close();
        System.out.println();
    }

    private static void runAuditHeadersDemo() throws InterruptedException {
        Properties props = createBaseProperties();
        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(props);

        String topic = "fund-transfer-events";

        System.out.println("-".repeat(70));
        System.out.println("PART 3: Banking Audit Headers");
        System.out.println("-".repeat(70));
        System.out.println("Regulatory compliance metadata for audit trails\n");

        CountDownLatch latch = new CountDownLatch(2);

        // Transaction 1: Internal transfer
        {
            Transaction txn = Transaction.builder()
                    .fromAccount("SAVINGS-001")
                    .toAccount("CURRENT-001")
                    .amount(new BigDecimal("100000.00"))
                    .type("INTERNAL_TRANSFER")
                    .build();

            List<Header> headers = createAuditHeaders(
                    "CORE-BANKING-V2",
                    "user-12345",
                    "BRANCH-MUMBAI-001",
                    "INTERNAL",
                    "192.168.1.100",
                    "MOBILE_APP",
                    "SESSION-ABC123"
            );

            ProducerRecord<String, Transaction> record = new ProducerRecord<>(
                    topic, null, System.currentTimeMillis(),
                    txn.getFromAccount(), txn, headers
            );

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("  Transaction 1: Internal Transfer");
                    System.out.printf("    Amount: ₹%s%n", txn.getAmount());
                    System.out.println("    Audit Headers:");
                    record.headers().forEach(h ->
                            System.out.printf("      %s: %s%n", h.key(), new String(h.value(), StandardCharsets.UTF_8)));
                    System.out.println();
                }
                latch.countDown();
            });
        }

        Thread.sleep(50);

        // Transaction 2: External RTGS
        {
            Transaction txn = Transaction.builder()
                    .fromAccount("CURRENT-001")
                    .toAccount("EXT-HDFC-12345")
                    .amount(new BigDecimal("500000.00"))
                    .type("RTGS")
                    .build();

            List<Header> headers = createAuditHeaders(
                    "PAYMENT-GATEWAY-V3",
                    "user-12345",
                    "BRANCH-MUMBAI-001",
                    "EXTERNAL",
                    "192.168.1.100",
                    "NET_BANKING",
                    "SESSION-ABC123"
            );
            // Additional RTGS-specific headers
            headers.add(new RecordHeader("beneficiary-bank", "HDFC0001234".getBytes(StandardCharsets.UTF_8)));
            headers.add(new RecordHeader("regulatory-report", "true".getBytes(StandardCharsets.UTF_8)));
            headers.add(new RecordHeader("aml-check-required", "true".getBytes(StandardCharsets.UTF_8)));

            ProducerRecord<String, Transaction> record = new ProducerRecord<>(
                    topic, null, System.currentTimeMillis(),
                    txn.getFromAccount(), txn, headers
            );

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("  Transaction 2: RTGS Transfer");
                    System.out.printf("    Amount: ₹%s%n", txn.getAmount());
                    System.out.println("    Audit Headers:");
                    record.headers().forEach(h ->
                            System.out.printf("      %s: %s%n", h.key(), new String(h.value(), StandardCharsets.UTF_8)));
                    System.out.println();
                }
                latch.countDown();
            });
        }

        latch.await();
        producer.close();
    }

    private static List<Header> createAuditHeaders(String sourceSystem, String userId,
                                                   String branchCode, String transferType, String clientIp, String channel, String sessionId) {

        List<Header> headers = new ArrayList<>();
        headers.add(new RecordHeader("source-system", sourceSystem.getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("user-id", userId.getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("branch-code", branchCode.getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("transfer-type", transferType.getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("client-ip", clientIp.getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("channel", channel.getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("session-id", sessionId.getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("event-time", Instant.now().toString().getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("correlation-id", UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)));
        return headers;
    }

    private static String generateSpanId() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 16);
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
        System.out.println("HEADERS & METADATA ANALYSIS");
        System.out.println("=".repeat(70));

        System.out.println("\nHEADER STRUCTURE:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ┌───────────────────────────────────────────────────────────────┐");
        System.out.println("  │                      Kafka Record                             │");
        System.out.println("  ├───────────────────────────────────────────────────────────────┤");
        System.out.println("  │  Topic:     fund-transfer-events                              │");
        System.out.println("  │  Partition: 3                                                 │");
        System.out.println("  │  Offset:    12345                                             │");
        System.out.println("  │  Timestamp: 1732451234567                                     │");
        System.out.println("  ├───────────────────────────────────────────────────────────────┤");
        System.out.println("  │  Key:       ACC001                                            │");
        System.out.println("  ├───────────────────────────────────────────────────────────────┤");
        System.out.println("  │  Headers:   ┌─────────────────┬─────────────────────────┐     │");
        System.out.println("  │             │ correlation-id  │ REQ-12345-ABCD          │     │");
        System.out.println("  │             │ source-system   │ CORE-BANKING            │     │");
        System.out.println("  │             │ user-id         │ user-12345              │     │");
        System.out.println("  │             │ trace-id        │ abc123def456...         │     │");
        System.out.println("  │             └─────────────────┴─────────────────────────┘     │");
        System.out.println("  ├───────────────────────────────────────────────────────────────┤");
        System.out.println("  │  Value:     {\"transactionId\": \"TXN-001\", ...}               │");
        System.out.println("  └───────────────────────────────────────────────────────────────┘");
        System.out.println("");

        System.out.println("\nHEADER API:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  // Adding headers to ProducerRecord");
        System.out.println("  List<Header> headers = new ArrayList<>();");
        System.out.println("  headers.add(new RecordHeader(\"key\", \"value\".getBytes(UTF_8)));");
        System.out.println("");
        System.out.println("  ProducerRecord<K, V> record = new ProducerRecord<>(");
        System.out.println("      topic,           // topic name");
        System.out.println("      partition,       // partition (null for auto)");
        System.out.println("      timestamp,       // message timestamp");
        System.out.println("      key,             // message key");
        System.out.println("      value,           // message value");
        System.out.println("      headers          // headers list");
        System.out.println("  );");
        System.out.println("");
        System.out.println("  // Reading headers in Consumer");
        System.out.println("  for (Header header : record.headers()) {");
        System.out.println("      String key = header.key();");
        System.out.println("      String value = new String(header.value(), UTF_8);");
        System.out.println("  }");
        System.out.println("");

        System.out.println("\nCOMMON HEADER PATTERNS:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ┌─────────────────────┬─────────────────────────────────────────┐");
        System.out.println("  │ Category            │ Headers                                 │");
        System.out.println("  ├─────────────────────┼─────────────────────────────────────────┤");
        System.out.println("  │ Tracing             │ trace-id, span-id, parent-span-id,      │");
        System.out.println("  │                     │ correlation-id, traceparent             │");
        System.out.println("  ├─────────────────────┼─────────────────────────────────────────┤");
        System.out.println("  │ Routing             │ priority, region, tenant-id,            │");
        System.out.println("  │                     │ destination-service                     │");
        System.out.println("  ├─────────────────────┼─────────────────────────────────────────┤");
        System.out.println("  │ Serialization       │ content-type, schema-id, version,       │");
        System.out.println("  │                     │ encoding                                │");
        System.out.println("  ├─────────────────────┼─────────────────────────────────────────┤");
        System.out.println("  │ Audit               │ user-id, client-ip, session-id,         │");
        System.out.println("  │                     │ source-system, event-time               │");
        System.out.println("  ├─────────────────────┼─────────────────────────────────────────┤");
        System.out.println("  │ Security            │ auth-token-hash, tenant-id,             │");
        System.out.println("  │                     │ encryption-key-id                       │");
        System.out.println("  └─────────────────────┴─────────────────────────────────────────┘");
        System.out.println("");

        System.out.println("\nDISTRIBUTED TRACING FLOW:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐");
        System.out.println("  │ API Gateway  │───►│ Payment Svc  │───►│ Fraud Check  │");
        System.out.println("  │              │    │              │    │              │");
        System.out.println("  │ trace: ABC   │    │ trace: ABC   │    │ trace: ABC   │");
        System.out.println("  │ span: 001    │    │ span: 002    │    │ span: 003    │");
        System.out.println("  │ parent: -    │    │ parent: 001  │    │ parent: 002  │");
        System.out.println("  └──────────────┘    └──────────────┘    └──────────────┘");
        System.out.println("         │                   │                   │");
        System.out.println("         ▼                   ▼                   ▼");
        System.out.println("  ┌────────────────────────────────────────────────────────┐");
        System.out.println("  │                      KAFKA                             │");
        System.out.println("  │  All messages carry same trace-id for correlation      │");
        System.out.println("  └────────────────────────────────────────────────────────┘");
        System.out.println("         │                   │                   │");
        System.out.println("         ▼                   ▼                   ▼");
        System.out.println("  ┌────────────────────────────────────────────────────────┐");
        System.out.println("  │                   OBSERVABILITY                        │");
        System.out.println("  │  Jaeger/Zipkin: Query by trace-id to see full flow     │");
        System.out.println("  └────────────────────────────────────────────────────────┘");
        System.out.println("");

        System.out.println("\nBANKING AUDIT REQUIREMENTS:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  Regulatory Compliance Headers:");
        System.out.println("");
        System.out.println("  ┌───────────────────────┬─────────────────────────────────────┐");
        System.out.println("  │ Header               │ Purpose                              │");
        System.out.println("  ├───────────────────────┼─────────────────────────────────────┤");
        System.out.println("  │ user-id              │ Who initiated the transaction        │");
        System.out.println("  │ client-ip            │ Where the request originated         │");
        System.out.println("  │ session-id           │ Session tracking                     │");
        System.out.println("  │ channel              │ Mobile/Web/Branch/ATM                │");
        System.out.println("  │ branch-code          │ Originating branch                   │");
        System.out.println("  │ event-time           │ Exact timestamp (ISO 8601)           │");
        System.out.println("  │ correlation-id       │ Link related transactions            │");
        System.out.println("  │ aml-check-required   │ Anti-money laundering flag           │");
        System.out.println("  │ regulatory-report    │ Needs regulatory reporting           │");
        System.out.println("  │ beneficiary-bank     │ External bank IFSC                   │");
        System.out.println("  └───────────────────────┴─────────────────────────────────────┘");
        System.out.println("");

        System.out.println("\nHEADERS vs PAYLOAD:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  PUT IN HEADERS:                    PUT IN PAYLOAD:");
        System.out.println("  ───────────────                    ───────────────");
        System.out.println("  ✓ Routing metadata                 ✓ Business data");
        System.out.println("  ✓ Tracing context                  ✓ Transaction details");
        System.out.println("  ✓ Audit metadata                   ✓ Domain-specific fields");
        System.out.println("  ✓ Schema information               ✓ Nested objects");
        System.out.println("  ✓ Security context                 ✓ Arrays, lists");
        System.out.println("");
        System.out.println("  BENEFITS OF HEADERS:");
        System.out.println("  • Inspect without deserializing payload");
        System.out.println("  • Route messages without parsing body");
        System.out.println("  • Smaller payload size");
        System.out.println("  • Separate concerns (infra vs business)");
        System.out.println("");

        System.out.println("\nHEADER BEST PRACTICES:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ✓ DO:");
        System.out.println("    • Keep header values small (< 1KB total)");
        System.out.println("    • Use consistent naming (kebab-case recommended)");
        System.out.println("    • Include correlation-id for tracing");
        System.out.println("    • Document all custom headers");
        System.out.println("    • Use UTF-8 encoding consistently");
        System.out.println("");
        System.out.println("  ✗ DON'T:");
        System.out.println("    • Store large data in headers");
        System.out.println("    • Duplicate payload data in headers");
        System.out.println("    • Use headers for sensitive credentials");
        System.out.println("    • Rely on header ordering");
        System.out.println("");

        System.out.println("\nCONFIG COMPARISON:");
        System.out.println("-".repeat(70));
        System.out.println("| Aspect              | Demo10           | Demo11              |");
        System.out.println("|---------------------|------------------|---------------------|");
        System.out.println("| Metadata            | Key only         | Key + Headers       |");
        System.out.println("| Tracing             | None             | trace-id, span-id   |");
        System.out.println("| Audit trail         | In payload       | Separate headers    |");
        System.out.println("| Routing info        | In key           | In headers          |");
        System.out.println("-".repeat(70));
    }
}