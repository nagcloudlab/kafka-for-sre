package com.example.security;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * Demo 1: Insecure Communication - Producer
 * Shows the security risk of transmitting sensitive payment data without encryption
 *
 * Banking Context: Payment Gateway sending transaction data
 * Risk: Card details, account numbers transmitted in plaintext
 */
public class Demo01_InsecureProducer {

    private static final String TOPIC = "payments";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        System.out.println("=".repeat(70));
        System.out.println("⚠️  SECURITY RISK: Sending payment WITHOUT encryption");
        System.out.println("=".repeat(70));

        // Create producer with NO security configuration
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // Note: NO security.protocol, NO ssl, NO sasl configurations

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            // Create sensitive payment data
            String paymentJson = createPaymentTransaction();

            System.out.println("\n📤 Sending payment transaction:");
            System.out.println(paymentJson);

            // Send to Kafka - completely unencrypted!
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    TOPIC,
                    "TXN20250125001",  // key
                    paymentJson        // value
            );

            RecordMetadata metadata = producer.send(record).get();

            System.out.println("\n✅ Sent to:");
            System.out.println("   Topic: " + metadata.topic());
            System.out.println("   Partition: " + metadata.partition());
            System.out.println("   Offset: " + metadata.offset());
            System.out.println("   Timestamp: " + metadata.timestamp());

            System.out.println("\n⚠️  WARNING: This data is transmitted in PLAINTEXT!");
            System.out.println("   ❌ Anyone on the network can intercept this");
            System.out.println("   ❌ Card details are completely exposed");
            System.out.println("   ❌ No authentication required to produce");
            System.out.println("   ❌ No encryption in transit");

        } catch (Exception e) {
            System.err.println("Error sending payment: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static String createPaymentTransaction() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode payment = mapper.createObjectNode();

            payment.put("transaction_id", "TXN20250125001");
            payment.put("from_account", "1234567890");
            payment.put("to_account", "0987654321");
            payment.put("amount", 50000.00);
            payment.put("currency", "INR");
            payment.put("card_number", "4532-1234-5678-9010");  // ⚠️ Exposed!
            payment.put("cvv", "123");                           // ⚠️ Exposed!
            payment.put("customer_name", "Rajesh Kumar");
            payment.put("bank_code", "HDFC0001234");
            payment.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME));

            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(payment);

        } catch (Exception e) {
            throw new RuntimeException("Error creating payment JSON", e);
        }
    }
}