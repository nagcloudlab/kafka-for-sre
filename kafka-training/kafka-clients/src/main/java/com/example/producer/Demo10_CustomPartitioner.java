package com.example.producer;

import com.example.Transaction;
import com.example.JsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * DEMO 10: Custom Partitioner
 *
 * WHAT CHANGED FROM DEMO 09:
 * +---------------------------+-------------------+---------------------+
 * | Aspect                    | Demo 09           | Demo 10             |
 * +---------------------------+-------------------+---------------------+
 * | Partitioner               | Default (murmur2) | Custom class        |
 * | Routing logic             | Hash-based        | Business rules      |
 * | Control                   | Limited           | Full control        |
 * +---------------------------+-------------------+---------------------+
 *
 * CUSTOM PARTITIONER INTERFACE:
 *
 * public interface Partitioner {
 *     void configure(Map<String, ?> configs);
 *     int partition(String topic, Object key, byte[] keyBytes,
 *                   Object value, byte[] valueBytes, Cluster cluster);
 *     void close();
 * }
 *
 * USE CASES FOR CUSTOM PARTITIONER:
 * 1. Region-based routing (data locality)
 * 2. Priority-based routing (SLA guarantees)
 * 3. Customer tier routing (premium vs standard)
 * 4. Time-based routing (batch windows)
 * 5. Regulatory compliance (data residency)
 *
 * Banking Scenario: Route by region for data locality, by priority for SLAs
 */
public class Demo10_CustomPartitioner {

    public static void main(String[] args) throws InterruptedException {

        System.out.println("=".repeat(70));
        System.out.println("DEMO 10: Custom Partitioner");
        System.out.println("=".repeat(70));
        System.out.println("Demonstrating business-logic based partition routing\n");

        // Part 1: Region-based partitioning
        runRegionBasedDemo();

        // Part 2: Priority-based partitioning
        runPriorityBasedDemo();

        printAnalysis();
    }

    private static void runRegionBasedDemo() throws InterruptedException {
        Properties props = createBaseProperties();

        // CHANGED: Use custom partitioner
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, RegionBasedPartitioner.class.getName());

        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(props);

        String topic = "fund-transfer-events";

        System.out.println("-".repeat(70));
        System.out.println("PART 1: Region-Based Partitioner");
        System.out.println("-".repeat(70));
        System.out.println("Routing: NORTH→P0,P1 | SOUTH→P2,P3 | WEST→P4,P5\n");

        String[][] regionAccounts = {
                {"NORTH-ACC001", "NORTH-ACC002", "NORTH-ACC003"},
                {"SOUTH-ACC101", "SOUTH-ACC102", "SOUTH-ACC103"},
                {"WEST-ACC201", "WEST-ACC202", "WEST-ACC203"}
        };

        Map<String, Map<Integer, Integer>> regionPartitionDist = new HashMap<>();
        int totalMessages = 0;

        for (String[] accounts : regionAccounts) {
            String region = accounts[0].split("-")[0];
            regionPartitionDist.put(region, new HashMap<>());

            for (String account : accounts) {
                totalMessages++;
            }
        }

        CountDownLatch latch = new CountDownLatch(totalMessages);

        for (String[] accounts : regionAccounts) {
            String region = accounts[0].split("-")[0];
            System.out.printf("  %s Region:%n", region);

            for (String account : accounts) {
                Transaction txn = Transaction.builder()
                        .fromAccount(account)
                        .toAccount("MERCHANT-001")
                        .amount(new BigDecimal("5000.00"))
                        .type("REGIONAL_TRANSFER")
                        .build();

                ProducerRecord<String, Transaction> record =
                        new ProducerRecord<>(topic, account, txn);

                final String reg = region;
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        regionPartitionDist.get(reg).merge(metadata.partition(), 1, Integer::sum);
                        System.out.printf("    %s → Partition: %d%n", record.key(), metadata.partition());
                    }
                    latch.countDown();
                });

                Thread.sleep(10);
            }
            System.out.println();
        }

        latch.await();
        producer.close();

        System.out.println("  Distribution Summary:");
        regionPartitionDist.forEach((region, dist) ->
                System.out.printf("    %s → Partitions: %s%n", region, dist.keySet()));
        System.out.println();
    }

    private static void runPriorityBasedDemo() throws InterruptedException {
        Properties props = createBaseProperties();

        // CHANGED: Use priority partitioner
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, PriorityPartitioner.class.getName());

        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(props);

        String topic = "fund-transfer-events";

        System.out.println("-".repeat(70));
        System.out.println("PART 2: Priority-Based Partitioner");
        System.out.println("-".repeat(70));
        System.out.println("Routing: HIGH→P0 | MEDIUM→P1,P2,P3 | LOW→P4,P5\n");

        // Simulate different priority transactions
        String[][] priorityTransactions = {
                {"HIGH-RTGS-001", "HIGH-RTGS-002", "HIGH-RTGS-003"},
                {"MEDIUM-NEFT-001", "MEDIUM-IMPS-002", "MEDIUM-NEFT-003"},
                {"LOW-NOTIFY-001", "LOW-LOG-002", "LOW-NOTIFY-003"}
        };

        Map<String, Map<Integer, Integer>> priorityPartitionDist = new HashMap<>();
        int totalMessages = 0;

        for (String[] txns : priorityTransactions) {
            String priority = txns[0].split("-")[0];
            priorityPartitionDist.put(priority, new HashMap<>());
            totalMessages += txns.length;
        }

        CountDownLatch latch = new CountDownLatch(totalMessages);

        for (String[] txns : priorityTransactions) {
            String priority = txns[0].split("-")[0];
            System.out.printf("  %s Priority:%n", priority);

            for (String txnKey : txns) {
                BigDecimal amount = switch (priority) {
                    case "HIGH" -> new BigDecimal("1000000.00");
                    case "MEDIUM" -> new BigDecimal("50000.00");
                    default -> new BigDecimal("100.00");
                };

                Transaction txn = Transaction.builder()
                        .fromAccount("ACC-" + txnKey)
                        .toAccount("ACC-BENEFICIARY")
                        .amount(amount)
                        .type(txnKey.split("-")[1])
                        .build();

                ProducerRecord<String, Transaction> record =
                        new ProducerRecord<>(topic, txnKey, txn);

                final String prio = priority;
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        priorityPartitionDist.get(prio).merge(metadata.partition(), 1, Integer::sum);
                        System.out.printf("    %s (₹%s) → Partition: %d%n",
                                record.key(), txn.getAmount(), metadata.partition());
                    }
                    latch.countDown();
                });

                Thread.sleep(10);
            }
            System.out.println();
        }

        latch.await();
        producer.close();

        System.out.println("  Distribution Summary:");
        priorityPartitionDist.forEach((priority, dist) ->
                System.out.printf("    %s → Partitions: %s%n", priority, dist.keySet()));
        System.out.println();
    }

    private static Properties createBaseProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        return props;
    }

    private static void printAnalysis() {
        System.out.println("=".repeat(70));
        System.out.println("CUSTOM PARTITIONER ANALYSIS");
        System.out.println("=".repeat(70));

        System.out.println("\nPARTITIONER INTERFACE:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  public class MyPartitioner implements Partitioner {");
        System.out.println("");
        System.out.println("      @Override");
        System.out.println("      public void configure(Map<String, ?> configs) {");
        System.out.println("          // Read custom configs (optional)");
        System.out.println("      }");
        System.out.println("");
        System.out.println("      @Override");
        System.out.println("      public int partition(String topic, Object key, byte[] keyBytes,");
        System.out.println("                           Object value, byte[] valueBytes, Cluster cluster) {");
        System.out.println("          // Return partition number (0 to numPartitions-1)");
        System.out.println("          return calculatePartition(key, cluster.partitionsForTopic(topic).size());");
        System.out.println("      }");
        System.out.println("");
        System.out.println("      @Override");
        System.out.println("      public void close() {");
        System.out.println("          // Cleanup resources");
        System.out.println("      }");
        System.out.println("  }");
        System.out.println("");

        System.out.println("\nREGION-BASED ROUTING VISUALIZATION:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ┌─────────────────────────────────────────────────────────────┐");
        System.out.println("  │                        PRODUCER                             │");
        System.out.println("  │                 RegionBasedPartitioner                      │");
        System.out.println("  └─────────────────────────────────────────────────────────────┘");
        System.out.println("                              │");
        System.out.println("           ┌──────────────────┼──────────────────┐");
        System.out.println("           │                  │                  │");
        System.out.println("           ▼                  ▼                  ▼");
        System.out.println("  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐");
        System.out.println("  │ NORTH           │ │ SOUTH           │ │ WEST            │");
        System.out.println("  │ Partitions 0, 1 │ │ Partitions 2, 3 │ │ Partitions 4, 5 │");
        System.out.println("  └─────────────────┘ └─────────────────┘ └─────────────────┘");
        System.out.println("           │                  │                  │");
        System.out.println("           ▼                  ▼                  ▼");
        System.out.println("  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐");
        System.out.println("  │ North Region    │ │ South Region    │ │ West Region     │");
        System.out.println("  │ Consumers       │ │ Consumers       │ │ Consumers       │");
        System.out.println("  └─────────────────┘ └─────────────────┘ └─────────────────┘");
        System.out.println("");

        System.out.println("\nPRIORITY-BASED ROUTING VISUALIZATION:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ┌─────────────────────────────────────────────────────────────┐");
        System.out.println("  │                        PRODUCER                             │");
        System.out.println("  │                  PriorityPartitioner                        │");
        System.out.println("  └─────────────────────────────────────────────────────────────┘");
        System.out.println("                              │");
        System.out.println("           ┌──────────────────┼──────────────────┐");
        System.out.println("           │                  │                  │");
        System.out.println("           ▼                  ▼                  ▼");
        System.out.println("  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐");
        System.out.println("  │ HIGH            │ │ MEDIUM          │ │ LOW             │");
        System.out.println("  │ Partition 0     │ │ Partitions 1-3  │ │ Partitions 4-5  │");
        System.out.println("  │ (Dedicated)     │ │ (Standard)      │ │ (Batch)         │");
        System.out.println("  └─────────────────┘ └─────────────────┘ └─────────────────┘");
        System.out.println("           │                  │                  │");
        System.out.println("           ▼                  ▼                  ▼");
        System.out.println("  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐");
        System.out.println("  │ Fast Consumer   │ │ Standard        │ │ Batch           │");
        System.out.println("  │ (SLA: 100ms)    │ │ Consumers       │ │ Consumers       │");
        System.out.println("  └─────────────────┘ └─────────────────┘ └─────────────────┘");
        System.out.println("");

        System.out.println("\nBANKING USE CASES:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ┌───────────────────────┬────────────────────────────────────┐");
        System.out.println("  │ Partitioner Type      │ Use Case                           │");
        System.out.println("  ├───────────────────────┼────────────────────────────────────┤");
        System.out.println("  │ Region-based          │ Data locality, compliance          │");
        System.out.println("  │ Priority-based        │ SLA guarantees, critical txns      │");
        System.out.println("  │ Customer-tier         │ Premium vs standard customers      │");
        System.out.println("  │ Branch-based          │ Branch-specific processing         │");
        System.out.println("  │ Time-window           │ Batch processing windows           │");
        System.out.println("  │ Amount-based          │ High-value transaction handling    │");
        System.out.println("  └───────────────────────┴────────────────────────────────────┘");
        System.out.println("");

        System.out.println("\nCUSTOM PARTITIONER BEST PRACTICES:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  ✓ DO:");
        System.out.println("    • Handle null keys gracefully");
        System.out.println("    • Return valid partition (0 to numPartitions-1)");
        System.out.println("    • Keep partition() method fast (called per message)");
        System.out.println("    • Handle partition count changes");
        System.out.println("    • Test with different partition counts");
        System.out.println("");
        System.out.println("  ✗ DON'T:");
        System.out.println("    • Make network calls in partition()");
        System.out.println("    • Throw exceptions (return fallback partition)");
        System.out.println("    • Hard-code partition numbers");
        System.out.println("    • Ignore cluster metadata");
        System.out.println("");

        System.out.println("\nDEFAULT vs CUSTOM PARTITIONER:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  Default Partitioner (Kafka 2.4+):");
        System.out.println("  ┌─────────────────────────────────────────────────────────────┐");
        System.out.println("  │ If key != null:                                             │");
        System.out.println("  │     partition = murmur2(key) % numPartitions                │");
        System.out.println("  │ If key == null:                                             │");
        System.out.println("  │     Sticky partitioner (stick to one until batch full)      │");
        System.out.println("  └─────────────────────────────────────────────────────────────┘");
        System.out.println("");
        System.out.println("  Custom Partitioner:");
        System.out.println("  ┌─────────────────────────────────────────────────────────────┐");
        System.out.println("  │ Any logic you want!                                         │");
        System.out.println("  │ - Route by region, priority, customer tier                  │");
        System.out.println("  │ - Time-based routing                                        │");
        System.out.println("  │ - Value-based routing (inspect message content)             │");
        System.out.println("  └─────────────────────────────────────────────────────────────┘");
        System.out.println("");

        System.out.println("\nCONFIGURATION:");
        System.out.println("-".repeat(70));
        System.out.println("");
        System.out.println("  // Use custom partitioner");
        System.out.println("  props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,");
        System.out.println("            \"com.example.producer.RegionBasedPartitioner\");");
        System.out.println("");
        System.out.println("  // Pass custom configs to partitioner");
        System.out.println("  props.put(\"partitioner.region.north\", \"0,1\");");
        System.out.println("  props.put(\"partitioner.region.south\", \"2,3\");");
        System.out.println("");

        System.out.println("\nCONFIG COMPARISON:");
        System.out.println("-".repeat(70));
        System.out.println("| Aspect              | Demo09 (Default) | Demo10 (Custom)     |");
        System.out.println("|---------------------|------------------|---------------------|");
        System.out.println("| Partitioner         | DefaultPartitioner| Custom class       |");
        System.out.println("| Routing logic       | murmur2 hash     | Business rules      |");
        System.out.println("| Flexibility         | Limited          | Full control        |");
        System.out.println("| Use case            | General purpose  | Specific routing    |");
        System.out.println("-".repeat(70));
    }
}