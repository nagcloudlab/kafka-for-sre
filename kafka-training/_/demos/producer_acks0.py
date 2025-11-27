#!/usr/bin/env python3
"""
Demo: Producer with acks=0 (Fire and Forget)
Risk Level: HIGHEST - Messages can be lost!
"""

from confluent_kafka import Producer
import json
import time

# Connect via EXTERNAL listener
BOOTSTRAP_SERVERS = 'localhost:9092,localhost:9093,localhost:9094'

def create_transfer(txn_id, amount):
    """Create a fund transfer message"""
    return {
        "txn_id": txn_id,
        "from_account": "1001234567",
        "to_account": "1009876543",
        "amount": amount,
        "currency": "INR",
        "type": "IMPS",
        "timestamp": int(time.time() * 1000)
    }

# Producer configuration with acks=0
config = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'acks': 0,  # Don't wait for any acknowledgment
    'linger.ms': 0
}

producer = Producer(config)

print("\n" + "="*65)
print("DEMO: Producer with acks=0 (NO ACKNOWLEDGMENT)")
print("="*65)
print("\nConfiguration:")
print("  acks = 0 (fire and forget)")
print("  Risk Level: HIGHEST ⚠️")
print("  Latency: LOWEST ⚡")
print("\nSending 5 fund transfer transactions...\n")

start_time = time.time()

for i in range(5):
    transfer = create_transfer(f"TXN_ACKS0_{i:03d}", 1000.00 + (i * 100))
    
    producer.produce(
        topic='fund-transfers',
        key=transfer['from_account'].encode('utf-8'),
        value=json.dumps(transfer).encode('utf-8')
    )
    
    print(f"Sent: {transfer['txn_id']} | Amount: ₹{transfer['amount']:.2f}")
    time.sleep(0.1)

# Flush to send all buffered messages
producer.flush()

elapsed = time.time() - start_time
print(f"\n⏱️  Total time: {elapsed:.3f} seconds")
print(f"📊 Average: {elapsed/5*1000:.1f}ms per message")

print("\n⚠️  WARNING: With acks=0, there's NO guarantee these messages")
print("   were actually received by Kafka!")
print("\n👉 Verify in Kafka UI: http://localhost:8080")
print("--"*65 + "\n")
