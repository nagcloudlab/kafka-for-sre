# Kafka Topics

## Objective
Learn how to create, list, describe, and delete Kafka topics using the Kafka command-line tools

## Prerequisites
- A running Kafka cluster
- Kafka command-line tools installed

## Steps
### 1. Create a Kafka Topic
To create a Kafka topic, use the following command:
```bash
# Fund Transfer - High volume
kafka1/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic fund-transfer-events \
  --partitions 6 --replication-factor 3

# Account Events - Account operations
kafka1/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic account-events \
  --partitions 3 --replication-factor 3

# Fraud Detection - Real-time alerts
kafka1/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic fraud-alerts \
  --partitions 4 --replication-factor 3

# Audit Logs - Compliance
kafka1/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic audit-logs \
  --partitions 3 --replication-factor 3
```

### 2. List Kafka Topics
To list all Kafka topics, use the following command:
```bash
kafka1/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### 3. Describe a Kafka Topic
To describe a specific Kafka topic, use the following command:
```bash
kafka1/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic fund-transfer-events
```

### 4. Delete a Kafka Topic
To delete a Kafka topic, use the following command:
```bash
kafka1/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --delete --topic audit-logs
```

## Topic Configuration Management
You can also modify topic configurations using the `kafka-configs.sh` tool. For example, to change the retention period of a topic:
```bash
kafka1/bin/kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name fund-transfer-events \
  --alter --add-config retention.ms=604800000
```

## Conclusion
You have successfully learned how to create, list, describe, and delete Kafka topics using the Kafka command-line tools. You also learned how to manage topic configurations.

