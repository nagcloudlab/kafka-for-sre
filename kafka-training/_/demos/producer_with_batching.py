#!/usr/bin/env python3
"""
Demo: Producer WITH Batching
Multiple messages per request - high throughput
"""

from confluent_kafka import Producer
import json
import time

BOOTSTRAP_SERVERS = 'localhost:9092,localhost:9093,localhost:9094'

def create_transfer(txn_id, amount):
    return {
        "txn_id": txn_id,
        "from_account": "5001234567",
        "to_account": "5009876543",
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

# Batching configuration for high throughput
config = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'acks': 'all',
    'enable.idempotence': True,
    'batch.size': 65536,    # 64KB batches
    'linger.ms': 10,        # Wait up to 10ms to fill batch
    'compression.type': 'none'  # No compression yet
}

producer = Producer(config)

NUM_MESSAGES = 1000

print("\n" + "="*65)
print("DEMO: Producer WITH Batching")
print("="*65)
print("\nConfiguration:")
print("  batch.size = 65536 (64KB)")
print("  linger.ms = 10 (wait 10ms for more messages)")
print(f"\nSending {NUM_MESSAGES} messages with batching...\n")

start_time = time.time()

for i in range(NUM_MESSAGES):
    transfer = create_transfer(f"BATCH_{i:04d}", 100.00)
    
    producer.produce(
        topic='fund-transfers',
        key=transfer['from_account'].encode('utf-8'),
        value=json.dumps(transfer).encode('utf-8'),
        callback=delivery_report
    )
    
    # Poll occasionally to trigger callbacks (not every message)
    if i % 100 == 0:
        producer.poll(0)

producer.flush()

elapsed = time.time() - start_time
throughput = NUM_MESSAGES / elapsed

print(f"\n" + "-"*65)
print(f"📊 RESULTS (With Batching):")
print(f"   Messages sent: {delivery_count}")
print(f"   Total time: {elapsed:.2f} seconds")
print(f"   Throughput: {throughput:.0f} messages/second")
print(f"   Latency per message: {elapsed/NUM_MESSAGES*1000:.2f} ms")
print("-"*65)
print("\n✅ Much higher throughput due to batching!")
print("="*65 + "\n")
