from kafka import KafkaProducer
import json, random, time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8")
)

PRODUCTS = ["laptop", "phone", "tablet", "headphones", "keyboard"]
STATUSES = ["placed", "confirmed", "shipped", "delivered", "cancelled"]
USER_POOL = [f"user_{i:04d}" for i in range(1, 100)]

def generate_event():
    return {
        "event_id": f"evt_{random.randint(100000, 999999)}",
        "user_id": random.choice(USER_POOL),
        "product": random.choice(PRODUCTS),
        "quantity": random.randint(1, 5),
        "unit_price": round(random.uniform(10, 1500), 2),
        "status": random.choice(STATUSES),
        "timestamp": datetime.utcnow().isoformat(),
        "region": random.choice(["north", "south", "east", "west"])
    }

print("🚀 Producing events...")

while True:
    event = generate_event()
    producer.send("order_events", key=event["user_id"], value=event)
    print(event)
    time.sleep(0.1)
