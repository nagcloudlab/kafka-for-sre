#!/usr/bin/env python3
"""
Demo: Consumer Group Member
Run multiple instances to see load balancing
"""

from confluent_kafka import Consumer, KafkaError
import json
import signal
import sys
import os

BOOTSTRAP_SERVERS = 'localhost:9092,localhost:9093,localhost:9094'

# Get instance ID from command line or environment
INSTANCE_ID = sys.argv[1] if len(sys.argv) > 1 else os.getenv('INSTANCE', '1')

running = True

def signal_handler(sig, frame):
    global running
    print(f"\n\n🛑 Consumer {INSTANCE_ID} shutting down...")
    running = False

signal.signal(signal.SIGINT, signal_handler)

def on_assign(consumer, partitions):
    """Callback when partitions are assigned"""
    partition_list = [p.partition for p in partitions]
    print(f"\n🎯 Consumer {INSTANCE_ID} ASSIGNED partitions: {partition_list}\n")

def on_revoke(consumer, partitions):
    """Callback when partitions are revoked (rebalance)"""
    partition_list = [p.partition for p in partitions]
    print(f"\n⚠️  Consumer {INSTANCE_ID} REVOKED partitions: {partition_list}")
    print("   (Rebalancing in progress...)\n")

config = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': 'payment-processor-group',  # SAME group for all instances
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True,
    'auto.commit.interval.ms': 5000,
    'session.timeout.ms': 10000,
    'heartbeat.interval.ms': 3000
}

consumer = Consumer(config)
consumer.subscribe(['fund-transfers'], on_assign=on_assign, on_revoke=on_revoke)

print("\n" + "="*65)
print(f"DEMO: Consumer Group Member (Instance {INSTANCE_ID})")
print("="*65)
print("\nConfiguration:")
print("  group.id = payment-processor-group")
print(f"  instance = {INSTANCE_ID}")
print("\nWaiting for partition assignment...\n")

message_count = 0

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
    
    message_count += 1
    value = json.loads(msg.value().decode('utf-8'))
    
    print(f"[C{INSTANCE_ID}] 📨 P{msg.partition()} | Offset {msg.offset():>5} | {value.get('txn_id', 'N/A')}")

consumer.close()
print(f"\n✅ Consumer {INSTANCE_ID} processed {message_count} messages")
print("="*65 + "\n")
