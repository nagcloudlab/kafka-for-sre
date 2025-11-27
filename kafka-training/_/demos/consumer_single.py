#!/usr/bin/env python3
"""
Demo: Single Consumer handling all partitions
"""

from confluent_kafka import Consumer, KafkaError
import json
import signal
import sys

BOOTSTRAP_SERVERS = 'localhost:9092,localhost:9093,localhost:9094'

running = True

def signal_handler(sig, frame):
    global running
    print("\n\n🛑 Shutting down consumer...")
    running = False

signal.signal(signal.SIGINT, signal_handler)

config = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': 'payment-processor-single',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'auto.commit.interval.ms': 5000
}

consumer = Consumer(config)
consumer.subscribe(['fund-transfers'])

print("\n" + "="*65)
print("DEMO: Single Consumer")
print("="*65)
print("\nConfiguration:")
print("  group.id = payment-processor-single")
print("  auto.offset.reset = earliest")
print("\nWaiting for partition assignment...\n")

message_count = 0
partitions_seen = set()

while running:
    msg = consumer.poll(timeout=1.0)
    
    if msg is None:
        continue
    
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(f"Error: {msg.error()}")
            break
    
    partitions_seen.add(msg.partition())
    message_count += 1
    
    value = json.loads(msg.value().decode('utf-8'))
    
    print(f"📨 Partition {msg.partition()} | Offset {msg.offset():>5} | {value.get('txn_id', 'N/A')}")
    
    if message_count % 20 == 0:
        print(f"\n📊 Stats: {message_count} messages from partitions {sorted(partitions_seen)}\n")

consumer.close()
print(f"\n✅ Total: {message_count} messages from partitions {sorted(partitions_seen)}")
print("="*65 + "\n")
