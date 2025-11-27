package com.example.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

/**
 * Custom Partitioner: Route messages based on region/zone
 *
 * PARTITIONING STRATEGY:
 * - NORTH region → Partitions 0, 1 (33% of partitions)
 * - SOUTH region → Partitions 2, 3 (33% of partitions)
 * - WEST region  → Partitions 4, 5 (33% of partitions)
 * - EAST region  → Fallback to hash-based
 *
 * KEY FORMAT: REGION-ACCOUNT_ID (e.g., NORTH-ACC001)
 *
 * WHY CUSTOM PARTITIONER:
 * 1. Data locality - Keep regional data together
 * 2. Load balancing - Control distribution
 * 3. Consumer affinity - Regional consumers process regional data
 * 4. Compliance - Keep data in specific partitions for regulatory reasons
 */
public class RegionBasedPartitioner implements Partitioner {

    @Override
    public void configure(Map<String, ?> configs) {
        // Can read custom configs here if needed
        // e.g., region-to-partition mappings from config
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {

        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        // If no key, use round-robin via sticky partitioner behavior
        if (keyBytes == null || key == null) {
            return Utils.toPositive(Utils.murmur2(valueBytes)) % numPartitions;
        }

        String keyStr = key.toString();

        // Extract region from key (format: REGION-ACCOUNT_ID)
        String region = extractRegion(keyStr);

        // Route based on region
        return switch (region) {
            case "NORTH" -> routeToPartitionRange(keyStr, 0, 1, numPartitions);
            case "SOUTH" -> routeToPartitionRange(keyStr, 2, 3, numPartitions);
            case "WEST"  -> routeToPartitionRange(keyStr, 4, 5, numPartitions);
            case "EAST"  -> routeToPartitionRange(keyStr, 0, numPartitions - 1, numPartitions);
            default      -> Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        };
    }

    private String extractRegion(String key) {
        if (key.contains("-")) {
            return key.split("-")[0].toUpperCase();
        }
        return "DEFAULT";
    }

    private int routeToPartitionRange(String key, int startPartition, int endPartition, int numPartitions) {
        // Ensure partitions exist
        if (startPartition >= numPartitions) {
            startPartition = 0;
        }
        if (endPartition >= numPartitions) {
            endPartition = numPartitions - 1;
        }

        // Hash within the range for even distribution
        int range = endPartition - startPartition + 1;
        int hash = Utils.toPositive(Utils.murmur2(key.getBytes()));
        return startPartition + (hash % range);
    }

    @Override
    public void close() {
        // Cleanup resources if any
    }
}