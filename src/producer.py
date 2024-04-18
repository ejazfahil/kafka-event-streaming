"""Kafka producer 2024-04-18"""
import json, uuid
from datetime import datetime, timezone
from confluent_kafka import Producer
from dataclasses import dataclass, asdict

@dataclass
class PaymentEvent:
    event_id: str
    user_id: str
    amount: float
    currency: str
    status: str
    created_at: str

def make_producer(brokers="localhost:9092"):
    return Producer({"bootstrap.servers": brokers,
                     "acks": "all",
                     "enable.idempotence": True,
                     "compression.type": "snappy"})

def emit(producer: Producer, topic: str, event: PaymentEvent):
    producer.produce(
        topic, key=event.user_id.encode(),
        value=json.dumps(asdict(event)).encode(),
        callback=lambda err, msg: print(f"[ERROR] {err}" if err else f"[OK] {msg.offset()}")
    )
    producer.poll(0)
