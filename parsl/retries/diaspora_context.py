from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def _collect_tail_records(consumer: Any, *, max_messages: int, timeout_ms: int) -> List[Any]:
    """Collect at most *max_messages* from the tail of each partition.

    Seeks each assigned partition to ``max(beginning, end - max_messages)``
    then polls once with the full timeout to read up to that end offset.
    """
    if max_messages <= 0:
        return []

    # Wait for partition assignment (up to 3 short polls).
    topic_partitions: List[Any] = []
    for _ in range(3):
        consumer.poll(timeout_ms=min(timeout_ms, 1000))
        topic_partitions = list(consumer.assignment())
        if topic_partitions:
            break
    if not topic_partitions:
        return []

    end_offsets = consumer.end_offsets(topic_partitions)
    beginning_offsets = consumer.beginning_offsets(topic_partitions)

    if logger.isEnabledFor(logging.DEBUG):
        offset_info = {
            f"{tp.topic}:{tp.partition}": {
                "beginning": int(beginning_offsets.get(tp, 0)),
                "end": int(end_offsets.get(tp, 0)),
            }
            for tp in topic_partitions
        }
        logger.debug("Diaspora offsets: %s", json.dumps(offset_info, sort_keys=True))

    # Seek each partition to at most max_messages before the end.
    for partition in topic_partitions:
        begin = int(beginning_offsets.get(partition, 0))
        end = int(end_offsets.get(partition, 0))
        consumer.seek(partition, max(begin, end - max_messages))

    # Single poll to read everything from the seek point to end.
    records: List[Any] = []
    polled = consumer.poll(timeout_ms=timeout_ms, max_records=max_messages)
    for partition_records in polled.values():
        records.extend(partition_records)

    records.sort(
        key=lambda r: (
            int(getattr(r, "timestamp", -1) or -1),
            int(getattr(r, "partition", -1) or -1),
            int(getattr(r, "offset", -1) or -1),
        )
    )
    return records[-max_messages:] if len(records) > max_messages else records


def fetch_diaspora_context(
    *,
    topic_name: str,
    run_id: Optional[str] = None,
    timeout_ms: int = 30000,
    max_messages: int = 100,
    environment: Optional[str] = None,
) -> Dict[str, Any]:
    """Fetch and normalize retry context from a Diaspora topic.

    When run_id is provided, collection starts at the first event whose
    ``run_id`` matches. That matching event and all following events are
    returned.
    """

    try:
        from diaspora_event_sdk import Client
        from diaspora_event_sdk import KafkaConsumer
    except Exception as exc:
        raise RuntimeError("diaspora_event_sdk is required for Diaspora context") from exc

    client = Client(environment=environment)
    kafka_topic = topic_name if "." in topic_name else f"{client.namespace}.{topic_name}"

    consumer = KafkaConsumer(
        kafka_topic,
        auto_offset_reset="earliest",
        consumer_timeout_ms=timeout_ms,
        enable_auto_commit=False,
    )

    tail_limit = max(0, int(max_messages))
    scanned = 0
    matched: List[Any] = []

    try:
        tail_records = _collect_tail_records(
            consumer,
            max_messages=tail_limit,
            timeout_ms=timeout_ms,
        )
        started_collecting = run_id is None
        for record in tail_records:
            try:
                value = json.loads(record.value.decode("utf-8", errors="replace"))
            except Exception:
                continue
            event = dict(value)

            if run_id is not None and not started_collecting:
                if str(event.get("run_id", "")) != run_id:
                    continue
                started_collecting = True

            scanned += 1
            matched.append(event)

            if len(matched) >= max_messages:
                break
    finally:
        consumer.close()

    return {
        "kafka_topic": kafka_topic,
        "run_id": run_id,
        "scanned": scanned,
        "matched_count": len(matched),
        "events": matched,
    }
