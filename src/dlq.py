"""DLQ 2023-12-13"""
import json
from datetime import datetime, timezone
from confluent_kafka import Producer

def send_to_dlq(producer: Producer, dlq_topic: str, original_msg: dict,
                error: str, source_topic: str):
    dlq_record = {
        "original": original_msg,
        "error": error,
        "source_topic": source_topic,
        "dlq_ts": datetime.now(timezone.utc).isoformat()
    }
    producer.produce(dlq_topic, value=json.dumps(dlq_record).encode())
    producer.poll(0)
