from kafka import KafkaProducer
import json
import time
import uuid
import random
import os
from datetime import datetime

# ============================================================
# CONFIG
# ============================================================

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

TOPIC = "input-topic"

# ============================================================
# CREATE PRODUCER
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=3
)

print("==================================")
print("Starting transaction producer...")
print(f"Broker = {BOOTSTRAP_SERVERS}")
print(f"Topic = {TOPIC}")
print("==================================")

# ============================================================
# SAMPLE DATA
# ============================================================

MERCHANTS = [
    "Amazon",
    "Walmart",
    "Netflix",
    "Spotify",
    "Apple",
    "Uber",
    "Airbnb",
    "Steam",
    "McDonalds",
    "Starbucks"
]

CITIES = [
    "New York",
    "London",
    "Tokyo",
    "Berlin",
    "Buenos Aires",
    "Paris",
    "Singapore"
]

PAYMENT_METHODS = [
    "CREDIT_CARD",
    "DEBIT_CARD",
    "PIX",
    "PAYPAL"
]

STATUSES = [
    "APPROVED",
    "DECLINED",
    "PENDING"
]

# ============================================================
# PRODUCE TRANSACTIONS
# ============================================================

try:

    while True:

        transaction = {
            "transactionId": str(uuid.uuid4()),
            "userId": f"user-{random.randint(1000, 9999)}",
            "amount": round(random.uniform(5, 2000), 2),
            "currency": "USD",
            "merchant": random.choice(MERCHANTS),
            "city": random.choice(CITIES),
            "paymentMethod": random.choice(PAYMENT_METHODS),
            "status": random.choice(STATUSES),
            "eventTime": datetime.utcnow().isoformat(),
            "deviceId": f"device-{random.randint(1, 500)}"
        }

        future = producer.send(TOPIC, value=transaction)

        metadata = future.get(timeout=10)

        print(
            f"Produced transaction: "
            f"{transaction['transactionId']} | "
            f"user={transaction['userId']} | "
            f"amount=${transaction['amount']} | "
            f"partition={metadata.partition} | "
            f"offset={metadata.offset}"
        )

        time.sleep(2)

except KeyboardInterrupt:
    print("\nStopping producer...")

finally:
    producer.flush()
    producer.close()