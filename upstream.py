"""Outbound calls to billing-rpc and the Kafka transcode topic."""

from __future__ import annotations

import json
import logging
import time

import requests
from kafka import KafkaProducer
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)


logger = logging.getLogger(__name__)

BILLING_RPC_URL = "https://billing-rpc.internal:8443/charge"
BILLING_TIMEOUT_S = 5.0
KAFKA_BOOTSTRAP = "kafka1.internal:9092,kafka2.internal:9092"
KAFKA_TOPIC = "image.transcode.requested"


# Producer with sensible reliability defaults: acks from all in-sync replicas,
# bounded retries with backoff, and a request timeout so a slow broker
# can't pin a request thread forever.
_PRODUCER = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=4,
    retry_backoff_ms=200,
    request_timeout_ms=5_000,
    linger_ms=20,
)


class CircuitOpen(RuntimeError):
    """Raised when billing-rpc has failed enough times that we are tripping
    the breaker. Callers should fail fast rather than queueing more load."""


# Simple in-memory breaker. Five consecutive failures opens the circuit
# for 30 seconds; the next call after that probes once. This is enough to
# keep request workers from piling up against a degraded billing-rpc.
_FAILURE_COUNT = 0
_OPEN_UNTIL_TS = 0.0
_FAIL_THRESHOLD = 5
_OPEN_FOR_S = 30.0


def _breaker_allow() -> bool:
    return time.time() >= _OPEN_UNTIL_TS


def _breaker_record(success: bool) -> None:
    global _FAILURE_COUNT, _OPEN_UNTIL_TS
    if success:
        _FAILURE_COUNT = 0
        return
    _FAILURE_COUNT += 1
    if _FAILURE_COUNT >= _FAIL_THRESHOLD:
        _OPEN_UNTIL_TS = time.time() + _OPEN_FOR_S
        logger.warning(
            "billing-rpc circuit OPEN for %.0fs after %d consecutive failures",
            _OPEN_FOR_S,
            _FAILURE_COUNT,
        )


@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.2, min=0.2, max=2.0),
    retry=retry_if_exception_type((requests.RequestException,)),
)
def _post_billing(payload: dict) -> None:
    resp = requests.post(
        BILLING_RPC_URL,
        json=payload,
        timeout=BILLING_TIMEOUT_S,
    )
    resp.raise_for_status()


def charge_credits(image_id: str, *, cost_cents: int) -> None:
    """Post a charge to billing-rpc with a timeout, bounded retries, and
    a circuit breaker so a slow upstream can't pin our worker threads."""
    if not _breaker_allow():
        raise CircuitOpen("billing-rpc circuit is open; failing fast")
    try:
        _post_billing({"image_id": image_id, "cost_cents": cost_cents})
        _breaker_record(success=True)
    except Exception:
        _breaker_record(success=False)
        raise


def publish_transcode_event(image_id: str, target_format: str, quality: int) -> None:
    # Fire and continue; the producer is already configured with acks="all"
    # and bounded retries, and we no longer block on a per-call flush.
    _PRODUCER.send(
        KAFKA_TOPIC,
        {
            "image_id": image_id,
            "target_format": target_format,
            "quality": quality,
        },
    )
