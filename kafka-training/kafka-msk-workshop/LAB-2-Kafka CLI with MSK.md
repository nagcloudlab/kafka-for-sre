



# Verify environment variables
echo "Bootstrap: $MSK_BOOTSTRAP"
echo "Profile: $AWS_PROFILE"

# If not set, run:
export AWS_PROFILE=kafka-workshop
export MSK_BOOTSTRAP="b-2-public.kafkaworkshopcluster.3llf2f.c4.kafka.ap-south-1.amazonaws.com:9198,b-1-public.kafkaworkshopcluster.3llf2f.c4.kafka.ap-south-1.amazonaws.com:9198,b-3-public.kafkaworkshopcluster.3llf2f.c4.kafka.ap-south-1.amazonaws.com:9198"

# Client properties file location
export CLIENT_CONFIG=/Users/nag/kafka-training/kafka-msk-workshop/client.properties

---
# section-1
---

# Create topic with specific partitions and replication
kafka-topics.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --command-config $CLIENT_CONFIG \
  --create \
  --topic fraud-alerts \
  --partitions 3 \
  --replication-factor 3

# Create topic with custom configs
kafka-topics.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --command-config $CLIENT_CONFIG \
  --create \
  --topic settlement-events \
  --partitions 6 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete


# Basic describe
kafka-topics.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --command-config $CLIENT_CONFIG \
  --describe \
  --topic payment-events

# Describe all topics
kafka-topics.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --command-config $CLIENT_CONFIG \
  --describe
```

**Expected Output:**
```
Topic: payment-events	TopicId: abc123	PartitionCount: 3	ReplicationFactor: 3	Configs: 
	Topic: payment-events	Partition: 0	Leader: 2	Replicas: 2,1,3	Isr: 2,1,3
	Topic: payment-events	Partition: 1	Leader: 1	Replicas: 1,3,2	Isr: 1,3,2
	Topic: payment-events	Partition: 2	Leader: 3	Replicas: 3,2,1	Isr: 3,2,1


# Increase retention to 7 days
kafka-configs.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --command-config $CLIENT_CONFIG \
  --alter \
  --entity-type topics \
  --entity-name payment-events \
  --add-config retention.ms=604800000

# Describe topic configs
kafka-configs.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --command-config $CLIENT_CONFIG \
  --describe \
  --entity-type topics \
  --entity-name payment-events      


# Increase partitions (cannot decrease!)
kafka-topics.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --command-config $CLIENT_CONFIG \
  --alter \
  --topic fraud-alerts \
  --partitions 6


# Delete topic (if delete.topic.enable=true)
kafka-topics.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --command-config $CLIENT_CONFIG \
  --delete \
  --topic test-connection



---
# section-1
---


#  List Consumer Groups
kafka-consumer-groups.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --command-config $CLIENT_CONFIG \
  --list


# Describe Consumer Group
# Detailed view with lag
kafka-consumer-groups.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --command-config $CLIENT_CONFIG \
  --describe \
  --group payment-processors
```

**Expected Output:**
```
GROUP              TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG    CONSUMER-ID                                HOST            CLIENT-ID
payment-processors payment-events  0          15              15              0      consumer-1-abc123                          /192.168.1.10   payment-consumer-abc
payment-processors payment-events  1          22              25              3      consumer-1-abc123                          /192.168.1.10   payment-consumer-abc
payment-processors payment-events  2          18              18              0      consumer-1-abc123                          /192.168.1.10   payment-consumer-abc


# State of all groups
kafka-consumer-groups.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --command-config $CLIENT_CONFIG \
  --describe \
  --all-groups \
  --state
```
**Output:**
```
GROUP              COORDINATOR (ID)          ASSIGNMENT-STRATEGY  STATE           #MEMBERS
payment-processors b-1.xxx:9098 (1)          range                Stable          1
fraud-checkers     b-2.xxx:9098 (2)          range                Empty           0


# Describe Group Members
kafka-consumer-groups.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --command-config $CLIENT_CONFIG \
  --describe \
  --group payment-processors \
  --members

kafka-consumer-groups.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --command-config $CLIENT_CONFIG \
  --describe \
  --group payment-processors \
  --members \
  --verbose  

--
# Section 3: Offset Management
--

## 3.1 View Current Offsets
kafka-consumer-groups.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --command-config $CLIENT_CONFIG \
  --describe \
  --group payment-processors \
  --offsets

# 3.2 Reset Offsets - Dry Run
## Always dry-run first!
kafka-consumer-groups.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --command-config $CLIENT_CONFIG \
  --group payment-processors \
  --topic payment-events \
  --reset-offsets \
  --to-earliest \
  --dry-run

## 3.3 Reset to Earliest
## Stop consumers first, then execute
kafka-consumer-groups.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --command-config $CLIENT_CONFIG \
  --group payment-processors \
  --topic payment-events \
  --reset-offsets \
  --to-earliest \
  --execute


## 3.4 Reset to Latest
kafka-consumer-groups.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --command-config $CLIENT_CONFIG \
  --group payment-processors \
  --topic payment-events \
  --reset-offsets \
  --to-latest \
  --execute

## 3.5 Reset to Specific Offset
## Reset partition 0 to offset 10
kafka-consumer-groups.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --command-config $CLIENT_CONFIG \
  --group payment-processors \
  --topic payment-events:0 \
  --reset-offsets \
  --to-offset 10 \
  --execute

## 3.6 Reset by Timestamp
## Reset to messages after specific time (ISO format or epoch)
kafka-consumer-groups.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --command-config $CLIENT_CONFIG \
  --group payment-processors \
  --topic payment-events \
  --reset-offsets \
  --to-datetime 2025-11-26T10:00:00.000 \
  --execute

## 3.7 Shift Offsets
## Move forward by 5 messages
kafka-consumer-groups.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --command-config $CLIENT_CONFIG \
  --group payment-processors \
  --topic payment-events \
  --reset-offsets \
  --shift-by 5 \
  --execute

# Move backward by 10 messages
kafka-consumer-groups.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --command-config $CLIENT_CONFIG \
  --group payment-processors \
  --topic payment-events \
  --reset-offsets \
  --shift-by -10 \
  --execute

# 3.8 Delete Consumer Group
## Group must be empty (no active consumers)
kafka-consumer-groups.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --command-config $CLIENT_CONFIG \
  --delete \
  --group old-payment-processors  

---
Section 4: Producer & Consumer CLI
---

## 4.1 Console Producer
## Simple producer
kafka-console-producer.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --producer.config $CLIENT_CONFIG \
  --topic payment-events

## Producer with key (key:value format)
kafka-console-producer.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --producer.config $CLIENT_CONFIG \
  --topic payment-events \
  --property "parse.key=true" \
  --property "key.separator=:"
```

Type messages:
```
ACC001:{"transactionId":"TXN-001","amount":5000}
ACC002:{"transactionId":"TXN-002","amount":10000}

## 4.2 Console Consumer
## From beginning
kafka-console-consumer.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --consumer.config $CLIENT_CONFIG \
  --topic payment-events \
  --from-beginning

## From beginning with max messages
kafka-console-consumer.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --consumer.config $CLIENT_CONFIG \
  --topic payment-events \
  --from-beginning \
  --max-messages 10

## With key and timestamp
kafka-console-consumer.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --consumer.config $CLIENT_CONFIG \
  --topic payment-events \
  --from-beginning \
  --property print.key=true \
  --property print.timestamp=true \
  --property print.partition=true \
  --property print.offset=true
```

**Output Format:**
```
CreateTime:1732645234567	Partition:0	Offset:5	ACC001	{"transactionId":"TXN-001",...}

## 4.3 Consumer from Specific Partition
kafka-console-consumer.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --consumer.config $CLIENT_CONFIG \
  --topic payment-events \
  --partition 0 \
  --offset 5

## 4.4 Consumer with Group
kafka-console-consumer.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --consumer.config $CLIENT_CONFIG \
  --topic payment-events \
  --group cli-test-group \
  --from-beginning

---
# Section 5: Cluster Information
--

## 5.1 Describe Cluster
kafka-broker-api-versions.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --command-config $CLIENT_CONFIG
## 5.2 Get Cluster ID
kafka-cluster.sh cluster-id \
  --bootstrap-server $MSK_BOOTSTRAP \
  --command-config $CLIENT_CONFIG
## 5.3 List Log Directories (Disk Usage)
kafka-log-dirs.sh \
  --bootstrap-server $MSK_BOOTSTRAP \
  --command-config $CLIENT_CONFIG \
  --describe \
  --topic-list payment-events  




  ---


  cat > ./msk-cli.sh << 'EOF'
#!/bin/bash

# MSK CLI Helper Script
export AWS_PROFILE=kafka-workshop
export MSK_BOOTSTRAP="b-2-public.kafkaworkshopcluster.3llf2f.c4.kafka.ap-south-1.amazonaws.com:9198,b-1-public.kafkaworkshopcluster.3llf2f.c4.kafka.ap-south-1.amazonaws.com:9198,b-3-public.kafkaworkshopcluster.3llf2f.c4.kafka.ap-south-1.amazonaws.com:9198"
export CLIENT_CONFIG=~/kafka-msk-tools/client.properties

case "$1" in
  topics)
    kafka-topics.sh --bootstrap-server $MSK_BOOTSTRAP --command-config $CLIENT_CONFIG --list
    ;;
  describe-topic)
    kafka-topics.sh --bootstrap-server $MSK_BOOTSTRAP --command-config $CLIENT_CONFIG --describe --topic $2
    ;;
  groups)
    kafka-consumer-groups.sh --bootstrap-server $MSK_BOOTSTRAP --command-config $CLIENT_CONFIG --list
    ;;
  describe-group)
    kafka-consumer-groups.sh --bootstrap-server $MSK_BOOTSTRAP --command-config $CLIENT_CONFIG --describe --group $2
    ;;
  lag)
    kafka-consumer-groups.sh --bootstrap-server $MSK_BOOTSTRAP --command-config $CLIENT_CONFIG --describe --group $2 | grep -E "GROUP|LAG"
    ;;
  consume)
    kafka-console-consumer.sh --bootstrap-server $MSK_BOOTSTRAP --consumer.config $CLIENT_CONFIG --topic $2 --from-beginning --max-messages ${3:-10}
    ;;
  produce)
    kafka-console-producer.sh --bootstrap-server $MSK_BOOTSTRAP --producer.config $CLIENT_CONFIG --topic $2
    ;;
  *)
    echo "Usage: msk-cli.sh {topics|describe-topic|groups|describe-group|lag|consume|produce} [args]"
    echo ""
    echo "Examples:"
    echo "  msk-cli.sh topics                        # List all topics"
    echo "  msk-cli.sh describe-topic payment-events # Describe topic"
    echo "  msk-cli.sh groups                        # List consumer groups"
    echo "  msk-cli.sh describe-group payment-processors"
    echo "  msk-cli.sh lag payment-processors        # Show lag only"
    echo "  msk-cli.sh consume payment-events 5      # Consume 5 messages"
    echo "  msk-cli.sh produce payment-events        # Start producer"
    ;;
esac
EOF

chmod +x ~/kafka-msk-tools/msk-cli.sh


# Add to PATH
export PATH=$PATH:~/kafka-msk-tools

# Use helper script
msk-cli.sh topics
msk-cli.sh describe-topic payment-events
msk-cli.sh groups
msk-cli.sh describe-group payment-processors
msk-cli.sh lag payment-processors
msk-cli.sh consume payment-events 5