import json
from datetime import datetime, timedelta
from collections import defaultdict
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='latest'
)

# Track recent transaction timestamps per user
user_transaction_times = defaultdict(list)

AMOUNT_THRESHOLD = 2000
WINDOW_SECONDS = 10
FREQUENCY_THRESHOLD = 5

print("Starting fraud detection consumer... Press Ctrl+C to stop.\n")

for message in consumer:
    txn = message.value
    user_id = txn['user_id']
    amount = txn['amount']
    timestamp = datetime.fromisoformat(txn['timestamp'])

    flags = []

    # Rule 1: High amount transaction
    if amount > AMOUNT_THRESHOLD:
        flags.append(f"HIGH AMOUNT (${amount})")

    # Rule 2: Too many transactions in a short window
    user_transaction_times[user_id].append(timestamp)
    # Keep only timestamps within the recent window
    cutoff = timestamp - timedelta(seconds=WINDOW_SECONDS)
    user_transaction_times[user_id] = [
        t for t in user_transaction_times[user_id] if t >= cutoff
    ]
    recent_count = len(user_transaction_times[user_id])
    if recent_count >= FREQUENCY_THRESHOLD:
        flags.append(f"HIGH FREQUENCY ({recent_count} txns in {WINDOW_SECONDS}s)")

    if flags:
        print(f"🚨 FLAGGED: {txn} -> {', '.join(flags)}")
    else:
        print(f"OK: {txn}")