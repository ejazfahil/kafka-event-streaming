"""Replay DLQ messages 2024-01-24"""
import json
from confluent_kafka import Consumer, Producer

def replay_dlq(brokers, dlq_topic, target_topic, group_id="replay-group", max_msgs=1000):
    consumer = Consumer({"bootstrap.servers":brokers,"group.id":group_id,"auto.offset.reset":"earliest"})
    producer = Producer({"bootstrap.servers":brokers,"acks":"all"})
    consumer.subscribe([dlq_topic])
    replayed = 0
    while replayed < max_msgs:
        msg = consumer.poll(1.0)
        if msg is None: break
        if msg.error(): continue
        record = json.loads(msg.value())
        producer.produce(target_topic, value=json.dumps(record["original"]).encode())
        replayed += 1
    producer.flush(); consumer.close()
    print(f"Replayed {replayed} messages from {dlq_topic} → {target_topic}")
