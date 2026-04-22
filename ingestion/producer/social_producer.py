import logging
import json
import time
from confluent_kafka import Producer
from ingestion.config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    PRODUCER_BATCH, PRODUCER_REALTIME,
    TOPIC_BATCH, TOPIC_REALTIME,
)

logger = logging.getLogger(__name__)

MAX_POLL_ATTEMPTS = 10


class SocialProducer:
    def __init__(self):
        base = {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        }
        self._producers = {
            TOPIC_BATCH: Producer({
                **base,
                **PRODUCER_BATCH,
                "enable.idempotence": True,
                "max.in.flight.requests.per.connection": 5,
            }),
            TOPIC_REALTIME: Producer({
                **base,
                **PRODUCER_REALTIME,
            }),
        }
        logger.info(
            "SocialProducer ready | batch=%s realtime=%s",
            TOPIC_BATCH, TOPIC_REALTIME,
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def send(self, topic: str, post: dict) -> None:
        if topic not in self._producers:
            raise ValueError(f"Unknown topic: {topic}")

        producer = self._producers[topic]

        # ── Serialize ─────────────────────────────────────────────────────────
        try:
            key = str(post["post_id"]).encode("utf-8")
            value = json.dumps(
                post,
                ensure_ascii=False,
                default=str,   # tránh crash với datetime, numpy, v.v.
            ).encode("utf-8")
        except (KeyError, TypeError, ValueError, OverflowError) as e:
            logger.error(
                "Serialize FAILED | post_id=%s err=%s",
                post.get("post_id"), e,
            )
            return

        # ── Produce with backpressure handling ────────────────────────────────
        for attempt in range(1, MAX_POLL_ATTEMPTS + 1):
            try:
                producer.produce(
                    topic=topic,
                    key=key,
                    value=value,
                    on_delivery=self._on_delivery,
                )
                break
            except BufferError:
                logger.warning(
                    "Producer queue full, polling (attempt %d/%d)...",
                    attempt, MAX_POLL_ATTEMPTS,
                )
                producer.poll(1)
        else:
            logger.error(
                "Drop message after %d attempts | post_id=%s",
                MAX_POLL_ATTEMPTS, post.get("post_id"),
            )
            return

        # trigger delivery callback
        producer.poll(0)

    def flush(self, timeout: float = 60.0) -> None:
        deadline = time.monotonic() + timeout
        for topic, p in self._producers.items():
            remaining_budget = max(0.0, deadline - time.monotonic())
            remaining = p.flush(remaining_budget)
            if remaining > 0:
                logger.warning(
                    "Flush timeout | topic=%s remaining=%d msgs",
                    topic, remaining,
                )
            else:
                logger.info("Flushed | topic=%s", topic)

    def close(self) -> None:
        self.flush()

    def __enter__(self):
        return self

    def __exit__(self, *_) -> None:
        self.close()

    # ── Delivery callback ─────────────────────────────────────────────────────

    @staticmethod
    def _on_delivery(err, msg) -> None:
        key = msg.key().decode() if msg.key() else None
        if err:
            logger.error(
                "Delivery FAILED | topic=%s key=%s err=%s",
                msg.topic(), key, err,
            )
        else:
            logger.debug(
                "Sent | topic=%-20s partition=%d offset=%d key=%s",
                msg.topic(), msg.partition(), msg.offset(), key,
            )