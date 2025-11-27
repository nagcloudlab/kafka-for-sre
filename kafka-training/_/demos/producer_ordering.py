#!/usr/bin/env python3
"""
Demo: Message Ordering with Kafka
Same key = Same partition = Guaranteed order
"""

from confluent_kafka import Producer
import json
import time

BOOTSTRAP_SERVERS = 'localhost:9092,localhost:9093,localhost:9094'

def delivery_report(err, msg):
    if err:
        print(f'    ❌ FAILED: {err}')
    else:
        print(f'    ✅ Partition {msg.partition()}, Offset {msg.offset()}')

# Idempotent producer for ordering guarantee
config = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'acks': 'all',
    'enable.idempotence': True
}

producer = Producer(config)

print("\n" + "="*65)
print("DEMO: Message Ordering Guarantee")
print("="*65)
print("\nKey Concept:")
print("  Same Key → Same Partition → Strict FIFO Order")
print("\n" + "-"*65)

# Scenario 1: Same customer - all transactions must be ordered
print("\n📱 SCENARIO 1: Single Customer Transactions")
print("   Customer Account: 3001234567")
print("   All transactions MUST be processed in order\n")

customer_account = "3001234567"
transactions = [
    {"step": 1, "action": "BALANCE_CHECK", "amount": 0},
    {"step": 2, "action": "DEBIT", "amount": 50000},
    {"step": 3, "action": "CREDIT_BENEFICIARY", "amount": 50000},
    {"step": 4, "action": "SMS_NOTIFICATION", "amount": 0},
]

for txn in transactions:
    message = {
        "account": customer_account,
        "step": txn["step"],
        "action": txn["action"],
        "amount": txn["amount"],
        "timestamp": int(time.time() * 1000)
    }
    
    print(f"  Step {txn['step']}: {txn['action']}")
    
    # Using account as KEY ensures all go to same partition
    producer.produce(
        topic='fund-transfers',
        key=customer_account.encode('utf-8'),  # Same key!
        value=json.dumps(message).encode('utf-8'),
        callback=delivery_report
    )
    producer.poll(0)
    time.sleep(0.1)

producer.flush()

print("\n" + "-"*65)
print("\n📱 SCENARIO 2: Multiple Customers - Parallel Processing")
print("   Different customers can be processed in parallel")
print("   But each customer's transactions stay ordered\n")

customers = ["CUST_A_111", "CUST_B_222", "CUST_C_333"]

for i, customer in enumerate(customers):
    message = {
        "account": customer,
        "action": "TRANSFER",
        "amount": 10000 * (i + 1),
        "timestamp": int(time.time() * 1000)
    }
    
    print(f"  Customer: {customer} | Amount: ₹{message['amount']:,}")
    
    producer.produce(
        topic='fund-transfers',
        key=customer.encode('utf-8'),  # Different keys
        value=json.dumps(message).encode('utf-8'),
        callback=delivery_report
    )
    producer.poll(0)

producer.flush()

print("\n" + "-"*65)
print("\n✅ ORDERING GUARANTEES:")
print("   • Same key (customer) → Same partition → Strict order")
print("   • Different keys → May go to different partitions → Parallel")
print("   • Within partition: FIFO guaranteed")
print("   • Across partitions: No order guarantee (by design)")
print("-"*65)
print("\n👉 Check Kafka UI - same account always in same partition")
print("="*65 + "\n")
