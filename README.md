# Real-Time Fraud Detection Pipeline

A real-time data pipeline that streams simulated transaction events through Apache Kafka 
and applies rule-based fraud detection as events arrive.

## What It Does
- Generates realistic transaction events (user ID, amount, location, timestamp) continuously
- Streams events through Apache Kafka in real time
- Applies detection rules as each transaction arrives:
  - Flags transactions above a set amount threshold
  - Flags users making an unusually high number of transactions within a short time window
- Prints flagged vs. normal transactions live to the console

## Tech Stack
Python, Apache Kafka, Docker, Docker Compose

## Architecture
Producer (generates transactions) → Kafka topic → Consumer (applies detection rules) → Flagged output

## How to Run
1. Start Kafka and Zookeeper:
docker-compose up -d
2. Create the Kafka topic:
docker exec -it realtime-fraud-detection-kafka-1 kafka-topics --create --topic transactions --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
3. Install dependencies:
pip install -r requirements.txt
4. In one terminal, start the consumer:
python3 consumer/detect_fraud.py
5. In another terminal, start the producer:
python3 producer/generate_transactions.py

## Sample Output
OK: {'user_id': 'user_6', 'amount': 927.6, ...}
🚨 FLAGGED: {'user_id': 'user_17', 'amount': 2717.4, ...} -> HIGH AMOUNT ($2717.4)

## Possible Next Steps
- Replace in-memory tracking with Spark Structured Streaming for scalable windowed aggregation
- Store flagged transactions in a database (Postgres/Redis) instead of console output
- Add a simple dashboard to visualize flagged transactions live