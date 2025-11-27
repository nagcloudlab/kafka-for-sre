package com.example.producer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Logging Interceptor: Log all messages before send and after acknowledgment
 *
 * USE CASE:
 * - Debug message flow
 * - Audit trail
 * - Development/testing visibility
 */
public class LoggingInterceptor implements ProducerInterceptor<String, Object> {

    private String clientId;
    private AtomicLong messageCount = new AtomicLong(0);

    @Override
    public void configure(Map<String, ?> configs) {
        this.clientId = (String) configs.get("client.id");
        System.out.println("  [LoggingInterceptor] Configured for client: " + clientId);
    }

    @Override
    public ProducerRecord<String, Object> onSend(ProducerRecord<String, Object> record) {
        long count = messageCount.incrementAndGet();

        // Log before sending
        System.out.printf("  [LOG][SEND #%d] Topic: %s, Key: %s, Partition: %s%n",
                count,
                record.topic(),
                record.key(),
                record.partition() != null ? record.partition() : "auto");

        // Return record unchanged (or modify if needed)
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            System.out.printf("  [LOG][ACK] Partition: %d, Offset: %d, Timestamp: %d%n",
                    metadata.partition(),
                    metadata.offset(),
                    metadata.timestamp());
        } else {
            System.err.printf("  [LOG][ERROR] %s: %s%n",
                    exception.getClass().getSimpleName(),
                    exception.getMessage());
        }
    }

    @Override
    public void close() {
        System.out.printf("  [LoggingInterceptor] Closed. Total messages: %d%n", messageCount.get());
    }
}