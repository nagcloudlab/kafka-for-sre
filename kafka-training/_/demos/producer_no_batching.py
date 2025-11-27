#!/usr/bin/env python3
"""
Demo: Producer WITHOUT Batching
Each message sent individually - high overhead
"""

from confluent_kafka import Producer
import json
import time

BOOTSTRAP_SERVERS = 'localhost:9092,localhost:9093,localhost:9094'

def create_transfer(txn_id, amount):
    return {
        "txn_id": txn_id,
        "from_account": "4001234567",
        "to_account": "4009876543",
        "amount": amount,
        "currency": "INR",
        "type": "IMPS",
        "timestamp": int(time.time() * 1000)
    }

delivery_count = 0

def delivery_report(err, msg):
    global delivery_count
    if not err:
        delivery_count += 1

# No batching configuration
config = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'acks': 'all',
    'enable.idempotence': True,
    'batch.size': 1,        # Minimum batch size (effectively no batching)
    'linger.ms': 0          # Send immediately
}

producer = Producer(config)

NUM_MESSAGES = 1000

print("\n" + "="*65)
print("DEMO: Producer WITHOUT Batching")
print("="*65)
print("\nConfiguration:")
print("  batch.size = 1 byte (no batching)")
print("  linger.ms = 0 (send immediately)")
print(f"\nSending {NUM_MESSAGES} messages individually...\n")

start_time = time.time()

for i in range(NUM_MESSAGES):
    transfer = create_transfer(f"NOBATCH_{i:04d}", 100.00)
    
    producer.produce(
        topic='fund-transfers',
        key=transfer['from_account'].encode('utf-8'),
        value=json.dumps(transfer).encode('utf-8'),
        callback=delivery_report
    )
    
    # Force send each message individually
    producer.poll(0)
    
    if (i + 1) % 200 == 0:
        print(f"  Sent {i + 1}/{NUM_MESSAGES} messages...")

producer.flush()

elapsed = time.time() - start_time
throughput = NUM_MESSAGES / elapsed

print(f"\n" + "-"*65)
print(f"📊 RESULTS (No Batching):")
print(f"   Messages sent: {delivery_count}")
print(f"   Total time: {elapsed:.2f} seconds")
print(f"   Throughput: {throughput:.0f} messages/second")
print(f"   Latency per message: {elapsed/NUM_MESSAGES*1000:.2f} ms")
print("-"*65)
print("\n⚠️  Low throughput due to individual network round trips")
print("="*65 + "\n")
