import logging
import json

from confluent_kafka import Producer

from ingestion.config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    PRODUCER_BATCH, PRODUCER_REALTIME,
    TOPIC_BATCH, TOPIC_REALTIME,
)

logger = logging.getLogger(__name__)


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
        logger.info("SocialProducer ready | batch=%s realtime=%s",
                    TOPIC_BATCH, TOPIC_REALTIME)

    def send(self, topic: str, post: dict) -> None:
        producer = self._producers[topic]

        key = post["post_id"].encode()

        value = json.dumps(post).encode("utf-8")

        producer.produce(
            topic=topic,
            key=key,
            value=value,
            on_delivery=self._on_delivery,
        )
        producer.poll(0)

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