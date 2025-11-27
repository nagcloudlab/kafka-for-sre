#!/usr/bin/env python3
"""
Demo: Producer WITHOUT Idempotency
Shows how retries can cause duplicate messages
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
    if err:
        print(f'  ❌ FAILED: {err}')
    else:
        print(f'  ✅ Delivered: Partition {msg.partition()}, Offset {msg.offset()}')

# Producer WITHOUT idempotence
config = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'acks': 'all',
    'enable.idempotence': False,  # Idempotence DISABLED
    'retries': 3,
    'retry.backoff.ms': 100
}

producer = Producer(config)

print("\n" + "="*65)
print("DEMO: Producer WITHOUT Idempotency")
print("="*65)
print("\nConfiguration:")
print("  enable.idempotence = False")
print("  retries = 3")
print("\nSimulating retry scenario (sending same message 3 times)...\n")

# Simulate what happens when producer retries
payment = create_transfer("PAYMENT_001", 50000.00)

print(f"Original Transaction: {payment['txn_id']} | Amount: ₹{payment['amount']:,.2f}")
print("\nSimulating 3 retry attempts (as if network ACK failed):\n")

for attempt in range(3):
    print(f"  Attempt {attempt + 1}: Sending {payment['txn_id']}...")
    
    producer.produce(
        topic='fund-transfers',
        key=payment['from_account'].encode('utf-8'),
        value=json.dumps(payment).encode('utf-8'),
        callback=delivery_report
    )
    producer.poll(0)
    time.sleep(0.2)

producer.flush()

print("\n" + "-"*65)
print("❌ PROBLEM: Same message written 3 times!")
print("💸 Customer charged: ₹50,000 × 3 = ₹1,50,000")
print("🔴 This is why idempotency is critical for payments!")
print("-"*65)
print("\n👉 Check Kafka UI - you'll see 3 identical messages")
print("="*65 + "\n")
