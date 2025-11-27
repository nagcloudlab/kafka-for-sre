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
 * DEMO 02: Synchronous Producer (acks=1)
 * 
 * WHAT CHANGED FROM DEMO 01:
 * +------------------+------------+------------+
 * | Config           | Demo 01    | Demo 02    |
 * +------------------+------------+------------+
 * | acks             | 0          | 1          |
 * | Blocking         | No         | Yes        |
 * | Confirmation     | None       | Leader     |
 * +------------------+------------+------------+
 * 
 * WHAT acks=1 MEANS:
 * - Producer waits for LEADER to write message to its log
 * - Does NOT wait for followers to replicate
 * - If leader crashes after ack but before replication → DATA LOSS
 * 
 * FLOW:
 * Producer ---> Leader (writes) ---> ACK returned
 *                  |
 *                  +--> Followers (async replication)
 * 
 * Banking Scenario: Session logs, balance inquiries, OTP requests
 */
public class Demo02_SyncAcksOne {

    public static void main(String[] args) {
        
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        
        // CHANGED: acks=1 (leader acknowledgment)
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        
        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(props);
        
        String topic = "fund-transfer-events";
        int messageCount = 1000;
        
        System.out.println("=".repeat(60));
        System.out.println("DEMO 02: Synchronous Producer (acks=1)");
        System.out.println("=".repeat(60));
        System.out.println("Config: acks=1, retries=0, BLOCKING send");
        System.out.println("Change: Now waiting for leader acknowledgment\n");
        
        long startTime = System.currentTimeMillis();
        int success = 0, failed = 0;
        
        for (int i = 1; i <= messageCount; i++) {
            Transaction txn = Transaction.builder()
                    .fromAccount("ACC" + (i % 100))
                    .toAccount("ACC" + ((i + 1) % 100))
                    .amount(new BigDecimal("1000.00"))
                    .type("OTP_REQUEST")
                    .build();
            
            ProducerRecord<String, Transaction> record = 
                new ProducerRecord<>(topic, txn.getTransactionId(), txn);
            
            try {
                // CHANGED: Blocking call with .get()
                RecordMetadata metadata = producer.send(record).get();
                success++;
                
                // Show first 3 messages with details
                if (i <= 3) {
                    System.out.printf("✓ Message %d → Partition: %d, Offset: %d%n",
                            i, metadata.partition(), metadata.offset());
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
        System.out.printf("Avg Latency     : %.2f ms/msg%n", duration / (double) messageCount);
        
        System.out.println("\n" + "-".repeat(60));
        System.out.println("COMPARISON: Demo 01 vs Demo 02");
        System.out.println("-".repeat(60));
        System.out.println("| Metric       | Demo01 (acks=0) | Demo02 (acks=1) |");
        System.out.println("|--------------|-----------------|-----------------|");
        System.out.println("| Throughput   | ~50,000 msg/s   | ~2,000 msg/s    |");
        System.out.println("| Latency      | ~0.02 ms        | ~0.5 ms         |");
        System.out.println("| Guarantee    | None            | Leader wrote    |");
        System.out.println("| Data Loss    | Any failure     | Leader failure  |");
        System.out.println("-".repeat(60));
    }
}