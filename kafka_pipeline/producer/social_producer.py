"""
SocialProducer
──────────────
Nhận topic + post dict đã normalize → serialize Avro → gửi Kafka.
Routing đã được quyết định ở tầng reader (by filename), producer chỉ gửi.
"""

import logging
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL,
    PRODUCER_BATCH, PRODUCER_REALTIME,
    TOPIC_BATCH, TOPIC_REALTIME,
)

logger = logging.getLogger(__name__)

SCHEMA_STR = (Path(__file__).parent.parent / "schema" / "social_post.avsc").read_text()


class SocialProducer:

    def __init__(self):
        registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
        self._serializer = AvroSerializer(
            schema_registry_client=registry,
            schema_str=SCHEMA_STR,
            to_dict=lambda post, ctx: post,
        )

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
        logger.info("SocialProducer ready | batch=%s realtime=%s",
                    TOPIC_BATCH, TOPIC_REALTIME)

    def send(self, topic: str, post: dict) -> None:
        """Gửi 1 post vào topic chỉ định."""
        producer = self._producers[topic]
        key      = post["post_id"].encode()
        value    = self._serializer(
            post, SerializationContext(topic, MessageField.VALUE)
        )
        producer.produce(
            topic=topic,
            key=key,
            value=value,
            on_delivery=self._on_delivery,
        )
        producer.poll(0)   # flush delivery callbacks (non-blocking)

    def flush(self, timeout: float = 60.0) -> None:
        for p in self._producers.values():
            p.flush(timeout)
        logger.info("All producers flushed.")

    @staticmethod
    def _on_delivery(err, msg) -> None:
        if err:
            logger.error("Delivery FAILED | topic=%s key=%s err=%s",
                         msg.topic(), msg.key(), err)
        else:
            logger.debug("Sent | topic=%-25s partition=%d offset=%d key=%s",
                         msg.topic(), msg.partition(), msg.offset(), msg.key())
