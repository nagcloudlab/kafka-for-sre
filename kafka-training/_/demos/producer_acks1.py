#!/usr/bin/env python3
"""
Demo: Producer with acks=1 (Leader Acknowledgment)
Risk Level: MEDIUM - Leader confirms, but followers might not have it
"""

from confluent_kafka import Producer
import json
import time

BOOTSTRAP_SERVERS = 'localhost:9092,localhost:9093,localhost:9094'

def create_transfer(txn_id, amount):
    return {
        "txn_id": txn_id,
        "from_account": "1001234567",
        "to_account": "1009876543",
        "amount": amount,
        "currency": "INR",
        "type": "IMPS",
        "timestamp": int(time.time() * 1000)
    }

def delivery_report(err, msg):
    """Callback - invoked once per message to report delivery status"""
    if err:
        print(f'  ❌ DELIVERY FAILED: {err}')
    else:
        print(f'  ✅ Leader confirmed: Partition {msg.partition()}, Offset {msg.offset()}')

# Producer with acks=1
config = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'acks': 1,  # Wait for leader acknowledgment only
    'linger.ms': 0,
    'request.timeout.ms': 5000
}

producer = Producer(config)

print("\n" + "="*65)
print("DEMO: Producer with acks=1 (LEADER ACKNOWLEDGMENT)")
print("="*65)
print("\nConfiguration:")
print("  acks = 1 (wait for leader only)")
print("  Risk Level: MEDIUM ⚠️")
print("  Latency: MEDIUM ⏱️")
print("\nSending 5 fund transfer transactions...\n")

start_time = time.time()

for i in range(5):
    transfer = create_transfer(f"TXN_ACKS1_{i:03d}", 2000.00 + (i * 100))
    
    print(f"Sending: {transfer['txn_id']} | Amount: ₹{transfer['amount']:.2f}")
    
    producer.produce(
        topic='fund-transfers',
        key=transfer['from_account'].encode('utf-8'),
        value=json.dumps(transfer).encode('utf-8'),
        callback=delivery_report
    )
    
    producer.poll(0)  # Trigger delivery callbacks
    time.sleep(0.1)

# Flush and wait for all callbacks
producer.flush()

elapsed = time.time() - start_time
print(f"\n⏱️  Total time: {elapsed:.3f} seconds")
print(f"📊 Average: {elapsed/5*1000:.1f}ms per message")

print("\n✅ All messages confirmed by LEADER broker")
print("⚠️  Risk: If leader fails before replicating, data could be lost")
print("\n👉 Verify in Kafka UI: http://localhost:8080")
print("="*65 + "\n")
