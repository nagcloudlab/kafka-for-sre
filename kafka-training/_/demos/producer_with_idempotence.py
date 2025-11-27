#!/usr/bin/env python3
"""
Demo: Producer WITH Idempotency
Shows how Kafka prevents duplicate messages
"""

from confluent_kafka import Producer
import json
import time

BOOTSTRAP_SERVERS = 'localhost:9092,localhost:9093,localhost:9094'

def create_transfer(txn_id, amount):
    return {
        "txn_id": txn_id,
        "from_account": "2001234567",  # Different account for clarity
        "to_account": "2009876543",
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

# Producer WITH idempotence enabled
config = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'acks': 'all',
    'enable.idempotence': True,  # ✅ Idempotence ENABLED
    'retries': 3,
    'retry.backoff.ms': 100,
    'max.in.flight.requests.per.connection': 5
}

producer = Producer(config)

print("\n" + "="*65)
print("DEMO: Producer WITH Idempotency")
print("="*65)
print("\nConfiguration:")
print("  enable.idempotence = True ✅")
print("  acks = all (required for idempotence)")
print("\nHow it works:")
print("  - Producer gets unique ID (PID) from broker")
print("  - Each message tagged with sequence number")
print("  - Broker detects and discards duplicates")
print("\nSending 5 unique transactions...\n")

start_time = time.time()

for i in range(5):
    payment = create_transfer(f"IDEM_TXN_{i:03d}", 25000.00 + (i * 5000))
    
    print(f"Sending: {payment['txn_id']} | Amount: ₹{payment['amount']:,.2f}")
    
    producer.produce(
        topic='fund-transfers',
        key=payment['from_account'].encode('utf-8'),
        value=json.dumps(payment).encode('utf-8'),
        callback=delivery_report
    )
    producer.poll(0)
    time.sleep(0.1)

producer.flush()

elapsed = time.time() - start_time

print(f"\n⏱️  Total time: {elapsed:.3f} seconds")
print("\n" + "-"*65)
print("✅ With idempotency enabled:")
print("   - Each message has unique (PID, Sequence) pair")
print("   - If network causes retry, broker detects duplicate")
print("   - Duplicate silently discarded, no double-charging!")
print("-"*65)

print("\n📊 Behind the scenes (Broker perspective):")
print("   Message 1: PID=12345, Seq=0 → ACCEPTED")
print("   Message 2: PID=12345, Seq=1 → ACCEPTED")
print("   Retry Msg1: PID=12345, Seq=0 → DUPLICATE, discarded!")
print("   Message 3: PID=12345, Seq=2 → ACCEPTED")

print("\n👉 Check Kafka UI - each transaction appears exactly ONCE")
print("="*65 + "\n")
