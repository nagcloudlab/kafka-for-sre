package com.example.consumer;

import com.example.Transaction;
import com.example.JsonDeserializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * DEMO C08: Graceful Shutdown
 *
 * SHUTDOWN CHALLENGES:
 * 1. Consumer in middle of poll() - blocks shutdown
 * 2. Uncommitted offsets - data loss on restart
 * 3. Rebalance listener cleanup - state not saved
 * 4. JVM shutdown hooks - need proper handling
 *
 * SOLUTION: wakeup() + try-catch + shutdown hook
 *
 * SHUTDOWN FLOW:
 * ┌─────────────────────────────────────────────────────────────────┐
 * │ 1. Shutdown signal received (Ctrl+C, SIGTERM)                   │
 * │    ↓                                                            │
 * │ 2. Shutdown hook triggered                                      │
 * │    ↓                                                            │
 * │ 3. consumer.wakeup() called                                     │
 * │    ↓                                                            │
 * │ 4. WakeupException thrown from poll()                           │
 * │    ↓                                                            │
 * │ 5. Catch WakeupException                                        │
 * │    ↓                                                            │
 * │ 6. Commit final offsets                                         │
 * │    ↓                                                            │
 * │ 7. consumer.close() - triggers rebalance                        │
 * │    ↓                                                            │
 * │ 8. Exit cleanly                                                 │
 * └─────────────────────────────────────────────────────────────────┘
 */
public class DemoC08_GracefulShutdown {

    private static final String TOPIC = "fund-transfer-events";
    private static final AtomicBoolean keepRunning = new AtomicBoolean(true);

    public static void main(String[] args) throws InterruptedException {
        System.out.println("=".repeat(70));
        System.out.println("DEMO C08: Graceful Shutdown");
        System.out.println("=".repeat(70));

        demonstrateBasicShutdown();
        demonstrateShutdownHook();
        printAnalysis();
    }

    private static void demonstrateBasicShutdown() throws InterruptedException {
        System.out.println("\n" + "-".repeat(70));
        System.out.println("PART 1: Basic Graceful Shutdown Pattern");
        System.out.println("-".repeat(70));

        String groupId = "shutdown-demo-" + System.currentTimeMillis();
        Properties props = createConsumerProperties(groupId);
        KafkaConsumer<String, Transaction> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(TOPIC));

        System.out.println("\nConsumer Pattern:");
        System.out.println("  try {");
        System.out.println("      while (keepRunning) {");
        System.out.println("          records = consumer.poll();");
        System.out.println("          process(records);");
        System.out.println("          consumer.commitSync();");
        System.out.println("      }");
        System.out.println("  } catch (WakeupException e) {");
        System.out.println("      // Ignore - expected on shutdown");
        System.out.println("  } finally {");
        System.out.println("      consumer.close(); // Always close!");
        System.out.println("  }");
        System.out.println("");

        AtomicBoolean running = new AtomicBoolean(true);
        int processedCount = 0;

        // Simulate shutdown after 3 polls
        Thread shutdownTrigger = new Thread(() -> {
            try {
                Thread.sleep(3000);
                System.out.println("\n>>> SHUTDOWN SIGNAL received <<<");
                running.set(false);
                consumer.wakeup(); // Interrupt poll()
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        shutdownTrigger.start();

        System.out.println("Starting consumer (will auto-shutdown in 3 seconds)...\n");

        try {
            while (running.get()) {
                ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(1000));

                if (!records.isEmpty()) {
                    System.out.printf("  Processing %d records...%n", records.count());
                    processedCount += records.count();

                    // Process records
                    for (ConsumerRecord<String, Transaction> record : records) {
                        // Simulate processing
                    }

                    // Commit after processing
                    consumer.commitSync();
                    System.out.println("  Committed offsets.");
                }
            }
        } catch (WakeupException e) {
            System.out.println("\n  [WakeupException caught] - This is expected during shutdown");
        } finally {
            System.out.println("  [FINALLY] Closing consumer...");
            consumer.close();
            System.out.printf("  [FINALLY] Consumer closed. Total processed: %d%n", processedCount);
        }

        shutdownTrigger.join();
    }

    private static void demonstrateShutdownHook() throws InterruptedException {
        System.out.println("\n" + "-".repeat(70));
        System.out.println("PART 2: Shutdown Hook Pattern");
        System.out.println("-".repeat(70));

        String groupId = "shutdown-hook-" + System.currentTimeMillis();
        Properties props = createConsumerProperties(groupId);
        KafkaConsumer<String, Transaction> consumer = new KafkaConsumer<>(props);

        final CountDownLatch shutdownLatch = new CountDownLatch(1);

        // Register shutdown hook
        Thread shutdownHook = new Thread(() -> {
            System.out.println("\n>>> JVM Shutdown Hook triggered <<<");
            System.out.println("  Calling consumer.wakeup()...");
            consumer.wakeup();
            try {
                shutdownLatch.await(); // Wait for consumer to finish
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        consumer.subscribe(Collections.singletonList(TOPIC));

        System.out.println("\nShutdown Hook registered.");
        System.out.println("In production: Press Ctrl+C to trigger graceful shutdown");
        System.out.println("For demo: Auto-triggering after 3 seconds...\n");

        // Simulate shutdown
        Thread demoShutdown = new Thread(() -> {
            try {
                Thread.sleep(3000);
                System.out.println("\n>>> Simulating shutdown (calling System.exit) <<<");
                // In real app: Ctrl+C or kill signal triggers shutdown hook
                consumer.wakeup();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        demoShutdown.start();

        int processedCount = 0;

        try {
            while (true) {
                ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(1000));

                if (!records.isEmpty()) {
                    System.out.printf("  Processing %d records...%n", records.count());
                    processedCount += records.count();
                    consumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            System.out.println("\n  [WakeupException] Shutdown in progress...");
        } finally {
            System.out.println("  [FINALLY] Committing final offsets...");
            try {
                consumer.commitSync();
                System.out.println("  [FINALLY] Final commit successful.");
            } catch (Exception e) {
                System.err.println("  [FINALLY] Final commit failed: " + e.getMessage());
            }

            System.out.println("  [FINALLY] Closing consumer...");
            consumer.close();
            System.out.printf("  [FINALLY] Graceful shutdown complete. Processed: %d%n", processedCount);

            shutdownLatch.countDown();
        }

        // Remove shutdown hook since we handled it manually
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
        demoShutdown.join();
    }

    private static Properties createConsumerProperties(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        props.put("json.deserializer.type", Transaction.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return props;
    }

    private static void printAnalysis() {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("GRACEFUL SHUTDOWN ANALYSIS");
        System.out.println("=".repeat(70));

        System.out.println("\nWHY GRACEFUL SHUTDOWN MATTERS:");
        System.out.println("-".repeat(70));
        System.out.println("WITHOUT graceful shutdown:");
        System.out.println("  ✗ Uncommitted offsets lost → Data loss or duplicates");
        System.out.println("  ✗ In-memory state not saved → Processing errors");
        System.out.println("  ✗ Rebalance listener not called → Cleanup skipped");
        System.out.println("  ✗ Long rebalance delay (session timeout)");
        System.out.println("");
        System.out.println("WITH graceful shutdown:");
        System.out.println("  ✓ All offsets committed → No data loss");
        System.out.println("  ✓ State persisted → Clean restart");
        System.out.println("  ✓ Immediate rebalance → Fast recovery");
        System.out.println("  ✓ Resources released properly");
        System.out.println("");

        System.out.println("\nPRODUCTION SHUTDOWN PATTERN:");
        System.out.println("-".repeat(70));
        System.out.println("public class PaymentConsumer {");
        System.out.println("    private final KafkaConsumer<String, Transaction> consumer;");
        System.out.println("    private final AtomicBoolean running = new AtomicBoolean(true);");
        System.out.println("");
        System.out.println("    public PaymentConsumer() {");
        System.out.println("        this.consumer = new KafkaConsumer<>(props);");
        System.out.println("        ");
        System.out.println("        // Register shutdown hook");
        System.out.println("        Runtime.getRuntime().addShutdownHook(new Thread(() -> {");
        System.out.println("            System.out.println(\"Shutdown signal received\");");
        System.out.println("            running.set(false);");
        System.out.println("            consumer.wakeup();");
        System.out.println("        }));");
        System.out.println("    }");
        System.out.println("");
        System.out.println("    public void run() {");
        System.out.println("        try {");
        System.out.println("            consumer.subscribe(Collections.singletonList(topic));");
        System.out.println("");
        System.out.println("            while (running.get()) {");
        System.out.println("                ConsumerRecords<String, Transaction> records = ");
        System.out.println("                    consumer.poll(Duration.ofMillis(1000));");
        System.out.println("");
        System.out.println("                for (ConsumerRecord<String, Transaction> record : records) {");
        System.out.println("                    processTransaction(record.value());");
        System.out.println("                }");
        System.out.println("");
        System.out.println("                consumer.commitSync();");
        System.out.println("            }");
        System.out.println("        } catch (WakeupException e) {");
        System.out.println("            // Expected on shutdown - ignore");
        System.out.println("        } catch (Exception e) {");
        System.out.println("            log.error(\"Consumer error\", e);");
        System.out.println("        } finally {");
        System.out.println("            try {");
        System.out.println("                consumer.commitSync();");
        System.out.println("            } catch (Exception e) {");
        System.out.println("                log.error(\"Final commit failed\", e);");
        System.out.println("            }");
        System.out.println("            consumer.close();");
        System.out.println("            log.info(\"Consumer closed gracefully\");");
        System.out.println("        }");
        System.out.println("    }");
        System.out.println("}");
        System.out.println("");

        System.out.println("\nwakeup() vs close():");
        System.out.println("-".repeat(70));
        System.out.println("┌─────────────────────┬────────────────────────────────────────┐");
        System.out.println("│ Method              │ Behavior                               │");
        System.out.println("├─────────────────────┼────────────────────────────────────────┤");
        System.out.println("│ consumer.wakeup()   │ • Thread-safe                          │");
        System.out.println("│                     │ • Interrupts poll()                    │");
        System.out.println("│                     │ • Throws WakeupException               │");
        System.out.println("│                     │ • Can be called from another thread    │");
        System.out.println("├─────────────────────┼────────────────────────────────────────┤");
        System.out.println("│ consumer.close()    │ • NOT thread-safe                      │");
        System.out.println("│                     │ • Commits offsets (if auto-commit)     │");
        System.out.println("│                     │ • Triggers rebalance                   │");
        System.out.println("│                     │ • Releases resources                   │");
        System.out.println("│                     │ • MUST be called from consumer thread  │");
        System.out.println("└─────────────────────┴────────────────────────────────────────┘");
        System.out.println("");

        System.out.println("\nSHUTDOWN CHECKLIST:");
        System.out.println("-".repeat(70));
        System.out.println("☑ Register JVM shutdown hook");
        System.out.println("☑ Call consumer.wakeup() from shutdown hook");
        System.out.println("☑ Catch WakeupException in main loop");
        System.out.println("☑ Commit offsets in finally block");
        System.out.println("☑ Close consumer in finally block");
        System.out.println("☑ Handle close() exceptions");
        System.out.println("☑ Log shutdown progress");
        System.out.println("☑ Set timeout on close() (default 30s)");
        System.out.println("");

        System.out.println("\nKUBERNETES SHUTDOWN:");
        System.out.println("-".repeat(70));
        System.out.println("When pod receives SIGTERM:");
        System.out.println("1. terminationGracePeriodSeconds: 30 (default)");
        System.out.println("2. SIGTERM sent to container");
        System.out.println("3. JVM shutdown hook triggered");
        System.out.println("4. Consumer wakeup() → commit → close()");
        System.out.println("5. If not done in 30s → SIGKILL (forceful)");
        System.out.println("");
        System.out.println("Recommendation: Set grace period > max.poll.interval.ms");
        System.out.println("  terminationGracePeriodSeconds: 60");
        System.out.println("-".repeat(70));
    }
}