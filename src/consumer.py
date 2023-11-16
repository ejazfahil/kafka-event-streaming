"""Kafka consumer 2023-11-16"""
import json
from confluent_kafka import Consumer, KafkaError

def make_consumer(brokers, group_id, topics):
    c = Consumer({"bootstrap.servers": brokers, "group.id": group_id,
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe(topics); return c

def consume_loop(consumer, handler, batch_size=100, timeout=1.0):
    batch = []
    while True:
        msg = consumer.poll(timeout)
        if msg is None: continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF: continue
            raise RuntimeError(msg.error())
        batch.append(json.loads(msg.value()))
        if len(batch) >= batch_size:
            handler(batch)
            consumer.commit()
            batch = []
