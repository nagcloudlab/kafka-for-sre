#!/usr/bin/env python3
"""
Producer to test consumer group load balancing
"""

from confluent_kafka import Producer
import json
import time

BOOTSTRAP_SERVERS = 'localhost:9092,localhost:9093,localhost:9094'

config = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'acks': 'all',
    'enable.idempotence': True
}

producer = Producer(config)

print("\n" + "="*65)
print("Producing messages to test consumer group...")
print("="*65 + "\n")

# Send messages with different keys to distribute across partitions
accounts = [
    "ACC_1111", "ACC_2222", "ACC_3333", 
    "ACC_4444", "ACC_5555", "ACC_6666"
]

for i in range(18):  # 18 messages, 3 per account
    account = accounts[i % 6]
    message = {
        "txn_id": f"GROUP_TEST_{i:03d}",
        "account": account,
        "amount": 1000 + (i * 100),
        "timestamp": int(time.time() * 1000)
    }
    
    producer.produce(
        topic='fund-transfers',
        key=account.encode('utf-8'),
        value=json.dumps(message).encode('utf-8')
    )
    
    print(f"Sent: {message['txn_id']} | Account: {account}")
    time.sleep(0.3)

producer.flush()
print("\n✅ All messages sent!")
print("="*65 + "\n")
