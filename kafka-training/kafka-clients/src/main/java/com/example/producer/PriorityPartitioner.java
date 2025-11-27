package com.example.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

/**
 * Custom Partitioner: Route messages based on priority/transaction type
 *
 * PARTITIONING STRATEGY:
 * - HIGH priority (RTGS, large amounts) → Partition 0 (dedicated, fast consumers)
 * - MEDIUM priority (NEFT, IMPS)        → Partitions 1, 2, 3
 * - LOW priority (notifications, logs)  → Partitions 4, 5
 *
 * KEY FORMAT: PRIORITY-ACCOUNT_ID (e.g., HIGH-ACC001)
 *
 * USE CASE:
 * - Partition 0 has dedicated high-performance consumers
 * - Critical transactions get priority processing
 * - Non-critical data doesn't slow down important transactions
 */
public class PriorityPartitioner implements Partitioner {

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {

        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        if (keyBytes == null || key == null) {
            return Utils.toPositive(Utils.murmur2(valueBytes)) % numPartitions;
        }

        String keyStr = key.toString();
        String priority = extractPriority(keyStr);

        return switch (priority) {
            case "HIGH"   -> 0;  // Dedicated partition for high priority
            case "MEDIUM" -> routeToRange(keyStr, 1, 3, numPartitions);
            case "LOW"    -> routeToRange(keyStr, 4, 5, numPartitions);
            default       -> Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        };
    }

    private String extractPriority(String key) {
        if (key.contains("-")) {
            return key.split("-")[0].toUpperCase();
        }
        return "MEDIUM";
    }

    private int routeToRange(String key, int start, int end, int numPartitions) {
        if (start >= numPartitions) start = 0;
        if (end >= numPartitions) end = numPartitions - 1;

        int range = end - start + 1;
        int hash = Utils.toPositive(Utils.murmur2(key.getBytes()));
        return start + (hash % range);
    }

    @Override
    public void close() {
    }
}