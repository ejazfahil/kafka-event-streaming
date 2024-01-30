"""Event tests 2024-01-30"""
import pytest, json
from src.producer import PaymentEvent, make_producer
from src.dlq import send_to_dlq
from unittest.mock import MagicMock

def test_payment_event_serialisation():
    e = PaymentEvent("id1","u1",49.99,"USD","completed","2024-01-01T00:00:00Z")
    d = {"event_id":"id1","user_id":"u1","amount":49.99,
         "currency":"USD","status":"completed","created_at":"2024-01-01T00:00:00Z"}
    import dataclasses; assert dataclasses.asdict(e) == d

def test_dlq_payload():
    mock_producer = MagicMock()
    send_to_dlq(mock_producer, "payments_dlq", {"id":"1"}, "parse error", "payments")
    mock_producer.produce.assert_called_once()
    args = mock_producer.produce.call_args
    payload = json.loads(args[1]["value"])
    assert payload["error"] == "parse error"
    assert payload["source_topic"] == "payments"
