import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

fake = Faker()
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

USER_IDS = [f"user_{i}" for i in range(1, 21)]  # 20 fake users

def generate_transaction():
    return {
        "user_id": random.choice(USER_IDS),
        "amount": round(random.uniform(5, 3000), 2),
        "location": fake.city(),
        "timestamp": datetime.utcnow().isoformat()
    }

if __name__ == "__main__":
    print("Starting transaction generator... Press Ctrl+C to stop.")
    while True:
        txn = generate_transaction()
        producer.send('transactions', value=txn)
        print(f"Sent: {txn}")
        time.sleep(random.uniform(0.5, 2))  # simulate real-time arrival