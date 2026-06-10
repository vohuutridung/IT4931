"""Elasticsearch indexer — stub sau khi bỏ Elasticsearch.

File được giữ lại để tránh ImportError từ các module cũ còn import ElasticsearchIndexer.
Tất cả các method đều là no-op.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class ElasticsearchIndexer:
    """No-op indexer — Elasticsearch đã bị loại bỏ để tiết kiệm tài nguyên."""

    def __init__(self, host: str = "") -> None:
        logger.info("ElasticsearchIndexer: running in no-op mode (ES removed)")

    def put_json(self, path: str, payload: dict) -> None:
        pass

    def post_json(self, path: str, payload, *, ndjson: bool = False) -> None:
        pass

    def ensure_indices(self) -> None:
        pass

    def bulk_index(self, index: str, docs: list[dict]) -> None:
        if docs:
            logger.debug("ElasticsearchIndexer.bulk_index: skipped %d docs for index %s (no-op)", len(docs), index)


def main() -> None:
    logger.info("ES indexer is disabled (no-op mode)")


if __name__ == "__main__":
    main()
