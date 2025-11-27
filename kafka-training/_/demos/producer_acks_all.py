#!/usr/bin/env python3
"""
Demo: Producer with acks=all (Full Replication)
Risk Level: LOWEST - All replicas confirm before acknowledgment
RECOMMENDED FOR: Banking transactions, fund transfers, payments
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
        "type": "RTGS",  # High-value transfer
        "timestamp": int(time.time() * 1000)
    }

def delivery_report(err, msg):
    if err:
        print(f'  ❌ DELIVERY FAILED: {err}')
    else:
        print(f'  ✅ FULLY REPLICATED: Partition {msg.partition()}, Offset {msg.offset()}')

# Producer with acks=all (RECOMMENDED FOR BANKING)
config = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'acks': 'all',  # Wait for ALL in-sync replicas
    'linger.ms': 0,
    'request.timeout.ms': 10000,
    'enable.idempotence': True  # Prevents duplicates on retry
}

producer = Producer(config)

print("\n" + "="*65)
print("DEMO: Producer with acks=all (FULL REPLICATION)")
print("="*65)
print("\nConfiguration:")
print("  acks = all (wait for ALL in-sync replicas)")
print("  enable.idempotence = true (prevent duplicates)")
print("  Risk Level: LOWEST ✅")
print("  Latency: HIGHEST ⏱️⏱️")
print("  Durability: MAXIMUM 🛡️")
print("\nSending 5 high-value RTGS transactions...\n")

start_time = time.time()

for i in range(5):
    transfer = create_transfer(f"TXN_RTGS_{i:03d}", 500000.00 + (i * 100000))
    
    print(f"Sending: {transfer['txn_id']} | Amount: ₹{transfer['amount']:,.2f}")
    
    producer.produce(
        topic='fund-transfers',
        key=transfer['from_account'].encode('utf-8'),
        value=json.dumps(transfer).encode('utf-8'),
        callback=delivery_report
    )
    
    producer.poll(0)
    time.sleep(0.1)

producer.flush()

elapsed = time.time() - start_time
print(f"\n⏱️  Total time: {elapsed:.3f} seconds")
print(f"📊 Average: {elapsed/5*1000:.1f}ms per message")

print("\n✅ Messages replicated across ALL in-sync replicas")
print("🛡️  Even if leader fails NOW, data is SAFE on followers")
print("💰 This is the CORRECT setting for fund transfers!")
print("\n👉 Verify in Kafka UI: http://localhost:8080")
print("="*65 + "\n")
