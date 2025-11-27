package com.example.security;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Demo 1: Insecure Communication - Consumer
 * Receives unencrypted payment data
 *
 * Banking Context: Payment Processor OR Malicious Actor
 * Risk: Anyone can consume this data without authentication
 */
public class Demo01_InsecureConsumer {

    private static final String TOPIC = "payments";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "payment-processor";

    public static void main(String[] args) {
        System.out.println("=".repeat(70));
        System.out.println("📥 Consuming payments WITHOUT authentication or encryption");
        System.out.println("=".repeat(70));

        // Create consumer with NO security configuration
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Note: NO security.protocol, NO ssl, NO sasl configurations

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

            consumer.subscribe(Collections.singletonList(TOPIC));

            System.out.println("\n⚠️  No authentication required - anyone can consume!");
            System.out.println("Waiting for messages...\n");

            ObjectMapper mapper = new ObjectMapper();
            int messageCount = 0;

            while (messageCount < 5) {  // Process 5 messages then exit
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    messageCount++;

                    System.out.println("─".repeat(70));
                    System.out.println("📨 Message " + messageCount + " received:");
                    System.out.println("   Topic: " + record.topic());
                    System.out.println("   Partition: " + record.partition());
                    System.out.println("   Offset: " + record.offset());
                    System.out.println("   Key: " + record.key());

                    // Parse and display payment details
                    try {
                        JsonNode payment = mapper.readTree(record.value());

                        System.out.println("\n⚠️  EXPOSED SENSITIVE DATA:");
                        System.out.println("   Transaction ID: " + payment.get("transaction_id").asText());
                        System.out.println("   Amount: ₹" + payment.get("amount").asDouble());
                        System.out.println("   Card Number: " + payment.get("card_number").asText());
                        System.out.println("   CVV: " + payment.get("cvv").asText());
                        System.out.println("   Customer: " + payment.get("customer_name").asText());
                        System.out.println("   From Account: " + payment.get("from_account").asText());
                        System.out.println("   To Account: " + payment.get("to_account").asText());

                    } catch (Exception e) {
                        System.err.println("   Error parsing payment: " + e.getMessage());
                    }

                    System.out.println("\n⚠️  This data was received in PLAINTEXT!");

                    if (messageCount >= 5) break;
                }
            }

            System.out.println("\n" + "=".repeat(70));
            System.out.println("⚠️  SECURITY PROBLEMS DEMONSTRATED:");
            System.out.println("   ❌ No encryption - network sniffing reveals all data");
            System.out.println("   ❌ No authentication - anyone can consume");
            System.out.println("   ❌ No authorization - no access control");
            System.out.println("   ❌ Compliance violation - PCI-DSS requires encryption");
            System.out.println("=".repeat(70));

        } catch (Exception e) {
            System.err.println("Error consuming payments: " + e.getMessage());
            e.printStackTrace();
        }
    }
}