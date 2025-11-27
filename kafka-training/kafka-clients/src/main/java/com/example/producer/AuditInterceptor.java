package com.example.producer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Audit Interceptor: Automatically add audit headers to all messages
 *
 * HEADERS ADDED:
 * - audit-timestamp: ISO 8601 timestamp
 * - audit-id: Unique audit ID
 * - audit-producer: Producer client ID
 * - audit-host: Hostname
 *
 * USE CASE:
 * - Compliance requirements
 * - Automatic audit trail
 * - Centralized audit policy
 */
public class AuditInterceptor implements ProducerInterceptor<String, Object> {

    private String clientId;
    private String hostname;

    @Override
    public void configure(Map<String, ?> configs) {
        //this.clientId = (String) configs.getOrDefault("client.id", "unknown");
        this.hostname = getHostname();
        System.out.println("  [AuditInterceptor] Configured - Host: " + hostname);
    }

    @Override
    public ProducerRecord<String, Object> onSend(ProducerRecord<String, Object> record) {
        // Add audit headers to every message
        record.headers().add(new RecordHeader("audit-timestamp",
                Instant.now().toString().getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("audit-id",
                UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("audit-producer",
                clientId.getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("audit-host",
                hostname.getBytes(StandardCharsets.UTF_8)));

        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // Audit acknowledgment if needed (e.g., write to audit log)
        if (exception != null) {
            System.err.printf("  [AUDIT][FAIL] Topic: %s, Error: %s%n",
                    metadata != null ? metadata.topic() : "unknown",
                    exception.getMessage());
        }
    }

    @Override
    public void close() {
        System.out.println("  [AuditInterceptor] Closed");
    }

    private String getHostname() {
        try {
            return java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown-host";
        }
    }
}