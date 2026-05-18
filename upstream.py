"""Outbound calls to billing-rpc and the Kafka transcode topic."""

from __future__ import annotations

import json

import requests
from kafka import KafkaProducer


BILLING_RPC_URL = "https://billing-rpc.internal:8443/charge"
KAFKA_BOOTSTRAP = "kafka1.internal:9092,kafka2.internal:9092"
KAFKA_TOPIC = "image.transcode.requested"


# A single shared producer (cheap) but with no acks / retries configured —
# any partition hiccup raises straight into the request handler.
_PRODUCER = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def charge_credits(image_id: str, *, cost_cents: int) -> None:
    """Post a charge to billing-rpc. No timeout, no retry, no circuit
    breaker — when billing-rpc slows down, /transcode blocks for as long
    as billing-rpc takes to respond. This is the #1 driver of the
    cascading failure we see during billing-rpc deploys."""
    requests.post(
        BILLING_RPC_URL,
        json={"image_id": image_id, "cost_cents": cost_cents},
    )


def publish_transcode_event(image_id: str, target_format: str, quality: int) -> None:
    _PRODUCER.send(
        KAFKA_TOPIC,
        {
            "image_id": image_id,
            "target_format": target_format,
            "quality": quality,
        },
    )
    _PRODUCER.flush()  # Synchronous flush per call — kills throughput.
