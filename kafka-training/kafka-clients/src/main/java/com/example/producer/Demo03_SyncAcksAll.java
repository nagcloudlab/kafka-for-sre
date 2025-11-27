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

/**
 * DEMO 03: Synchronous Producer (acks=all)
 *
 * WHAT CHANGED FROM DEMO 02:
 * +------------------+------------+------------+
 * | Config           | Demo 02    | Demo 03    |
 * +------------------+------------+------------+
 * | acks             | 1          | all        |
 * | Waits for        | Leader     | All ISR    |
 * | Durability       | Medium     | Highest    |
 * +------------------+------------+------------+
 *
 * WHAT acks=all MEANS:
 * - Producer waits for ALL in-sync replicas (ISR) to acknowledge
 * - Message is written to leader AND all followers in ISR
 * - Combined with min.insync.replicas=2, guarantees N copies
 *
 * FLOW:
 * Producer ---> Leader ---> Followers (ISR)
 *                              |
 *                              +--> All confirm
 *                              |
 *           <--- ACK ----------+
 *
 * ISR (In-Sync Replicas):
 * - Replicas that are fully caught up with leader
 * - Controlled by replica.lag.time.max.ms (default 30s)
 * - If follower falls behind, removed from ISR
 *
 * CRITICAL CONFIG COMBINATION:
 * +-------------------------+-------+--------------------------------+
 * | Config                  | Value | Purpose                        |
 * +-------------------------+-------+--------------------------------+
 * | acks                    | all   | Wait for all ISR               |
 * | min.insync.replicas     | 2     | At least 2 replicas must ack   |
 * | replication.factor      | 3     | Total copies of data           |
 * +-------------------------+-------+--------------------------------+
 *
 * With RF=3, min.insync=2, acks=all:
 * - Can tolerate 1 broker failure
 * - Always have 2 copies before ack
 *
 * Banking Scenario: Fund transfers, RTGS, audit logs, compliance events
 */
public class Demo03_SyncAcksAll {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

        // CHANGED: acks=all (all ISR acknowledgment)
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);

        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(props);

        String topic = "fund-transfer-events";
        int messageCount = 1000;

        System.out.println("=".repeat(60));
        System.out.println("DEMO 03: Synchronous Producer (acks=all)");
        System.out.println("=".repeat(60));
        System.out.println("Config: acks=all, retries=0, BLOCKING send");
        System.out.println("Change: Now waiting for ALL in-sync replicas\n");

        System.out.println("PRE-CHECK: Verify topic has min.insync.replicas=2");
        System.out.println("Run: kafka1/bin/kafka-configs.sh --bootstrap-server localhost:9092 \\");
        System.out.println("       --entity-type topics --entity-name fund-transfer-events --describe\n");

        long startTime = System.currentTimeMillis();
        int success = 0, failed = 0;
        long minLatency = Long.MAX_VALUE, maxLatency = 0, totalLatency = 0;

        for (int i = 1; i <= messageCount; i++) {
            Transaction txn = Transaction.builder()
                    .fromAccount("ACC" + (i % 100))
                    .toAccount("ACC" + ((i + 1) % 100))
                    .amount(new BigDecimal("500000.00"))
                    .type("RTGS")
                    .build();

            ProducerRecord<String, Transaction> record =
                    new ProducerRecord<>(topic, txn.getTransactionId(), txn);

            try {
                long sendStart = System.currentTimeMillis();
                RecordMetadata metadata = producer.send(record).get(); // sync, blocks until acked
                long latency = System.currentTimeMillis() - sendStart;

                success++;
                totalLatency += latency;
                minLatency = Math.min(minLatency, latency);
                maxLatency = Math.max(maxLatency, latency);

                if (i <= 3) {
                    System.out.printf("✓ Message %d → Partition: %d, Offset: %d, Latency: %d ms%n",
                            i, metadata.partition(), metadata.offset(), latency);
                }

            } catch (Exception e) {
                failed++;
                System.err.printf("✗ Message %d FAILED: %s%n", i, e.getMessage());
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        producer.close();

        System.out.println("\n" + "=".repeat(60));
        System.out.println("RESULTS");
        System.out.println("=".repeat(60));
        System.out.printf("Success/Failed  : %d / %d%n", success, failed);
        System.out.printf("Total Time      : %d ms%n", duration);
        System.out.printf("Throughput      : %d msg/sec%n", (messageCount * 1000 / duration));
        System.out.printf("Avg Latency     : %.2f ms/msg%n", totalLatency / (double) success);
        System.out.printf("Min Latency     : %d ms%n", minLatency);
        System.out.printf("Max Latency     : %d ms%n", maxLatency);

        System.out.println("\n" + "-".repeat(60));
        System.out.println("COMPARISON: Demo 01 vs Demo 02 vs Demo 03");
        System.out.println("-".repeat(60));
        System.out.println("| Metric       | Demo01    | Demo02    | Demo03      |");
        System.out.println("|              | acks=0    | acks=1    | acks=all    |");
        System.out.println("|--------------|-----------|-----------|-------------|");
        System.out.println("| Throughput   | ~50,000/s | ~2,000/s  | ~500-1000/s |");
        System.out.println("| Latency      | ~0.02 ms  | ~0.5 ms   | ~1-5 ms     |");
        System.out.println("| Waits for    | Nothing   | Leader    | All ISR     |");
        System.out.println("| Data Loss    | Anytime   | Leader ↓  | All ISR ↓   |");
        System.out.println("-".repeat(60));

        System.out.println("\n" + "-".repeat(60));
        System.out.println("DURABILITY GUARANTEE:");
        System.out.println("-".repeat(60));
        System.out.println("With: RF=3, min.insync.replicas=2, acks=all");
        System.out.println("");
        System.out.println("  Broker 1 (Leader)  ──┐");
        System.out.println("  Broker 2 (Follower)──┼── All must ACK before success");
        System.out.println("  Broker 3 (Follower)──┘");
        System.out.println("");
        System.out.println("  ✓ Can lose 1 broker → No data loss");
        System.out.println("  ✗ Lose 2 brokers   → Producer blocked (not enough ISR)");
        System.out.println("-".repeat(60));
    }
}