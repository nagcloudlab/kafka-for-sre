#!/usr/bin/env python3
"""
Demo: Producer with Batching + Compression
Maximum throughput configuration
"""

from confluent_kafka import Producer
import json
import time

BOOTSTRAP_SERVERS = 'localhost:9092,localhost:9093,localhost:9094'

def create_transfer(txn_id, amount):
    return {
        "txn_id": txn_id,
        "from_account": "6001234567",
        "to_account": "6009876543",
        "amount": amount,
        "currency": "INR",
        "type": "NEFT",
        "ifsc_from": "HDFC0001234",
        "ifsc_to": "ICIC0005678",
        "remarks": "Fund transfer for invoice payment",
        "timestamp": int(time.time() * 1000)
    }

delivery_count = 0

def delivery_report(err, msg):
    global delivery_count
    if not err:
        delivery_count += 1

# High throughput configuration with compression
config = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'acks': 'all',
    'enable.idempotence': True,
    'batch.size': 131072,       # 128KB batches
    'linger.ms': 20,            # Wait 20ms for batch
    'compression.type': 'lz4',  # LZ4 compression
    # 'buffer.memory': 67108864   # 64MB buffer

    'queue.buffering.max.kbytes': 33554,  # ~32 MB
    'queue.buffering.max.messages': 100000,
    'batch.num.messages': 1000,           # Python equivalent of batching

}

producer = Producer(config)

NUM_MESSAGES = 5000

print("\n" + "="*65)
print("DEMO: Producer with Batching + LZ4 Compression")
print("="*65)
print("\nConfiguration:")
print("  batch.size = 131072 (128KB)")
print("  linger.ms = 20 (wait 20ms)")
print("  compression.type = lz4")
print(f"\nSending {NUM_MESSAGES} messages...\n")

# Calculate uncompressed size
sample_msg = json.dumps(create_transfer("SAMPLE", 100.00))
uncompressed_size = len(sample_msg) * NUM_MESSAGES

start_time = time.time()

for i in range(NUM_MESSAGES):
    transfer = create_transfer(f"COMPRESSED_{i:05d}", 100.00 + i)
    
    producer.produce(
        topic='fund-transfers',
        key=transfer['from_account'].encode('utf-8'),
        value=json.dumps(transfer).encode('utf-8'),
        callback=delivery_report
    )
    
    if i % 500 == 0:
        producer.poll(0)
        if i > 0:
            print(f"  Sent {i}/{NUM_MESSAGES} messages...")

producer.flush()

elapsed = time.time() - start_time
throughput = NUM_MESSAGES / elapsed

print(f"\n" + "-"*65)
print(f"📊 RESULTS (Batching + LZ4 Compression):")
print(f"   Messages sent: {delivery_count}")
print(f"   Total time: {elapsed:.2f} seconds")
print(f"   Throughput: {throughput:.0f} messages/second")
print(f"   Uncompressed size: {uncompressed_size/1024:.1f} KB")
print(f"   Estimated compressed: ~{uncompressed_size/1024*0.3:.1f} KB (70% reduction)")
print("-"*65)

print("\n✅ BENEFITS:")
print("   • High throughput from batching")
print("   • Reduced network bandwidth from compression")
print("   • Less disk storage on brokers")
print("   • Faster replication between brokers")
print("="*65 + "\n")
