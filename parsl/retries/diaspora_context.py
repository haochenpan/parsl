from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def fetch_diaspora_context(
    *,
    topic_name: str,
    time_horizon: int,
    timeout_ms: int = 30000,
    max_messages: int = 10000,
    environment: Optional[str] = None,
) -> Dict[str, Any]:
    """Fetch retry context from a Diaspora topic starting at a unix timestamp.

    For each partition, seeks to the offset corresponding to *time_horizon*
    (a unix-epoch **millisecond** timestamp) and replays all messages from
    that point forward.

    Returns a dict with topic metadata and a list of decoded events.
    """
    try:
        from diaspora_event_sdk import Client, KafkaConsumer
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

    events: List[Dict[str, Any]] = []

    try:
        # Wait for partition assignment (up to 3 short polls).
        topic_partitions: List[Any] = []
        for _ in range(3):
            consumer.poll(timeout_ms=min(timeout_ms, 1000))
            topic_partitions = list(consumer.assignment())
            if topic_partitions:
                break
        if not topic_partitions:
            logger.warning("No partitions assigned for topic %s", kafka_topic)
            return {"kafka_topic": kafka_topic, "events": []}

        # Map each partition to the target timestamp for offset lookup.
        timestamp_map = {tp: time_horizon for tp in topic_partitions}
        offset_map = consumer.offsets_for_times(timestamp_map)

        end_offsets = consumer.end_offsets(topic_partitions)

        for tp in topic_partitions:
            offset_and_ts = offset_map.get(tp)
            if offset_and_ts is not None:
                consumer.seek(tp, offset_and_ts.offset)
            else:
                # No message at or after the timestamp — seek to end (nothing to read).
                consumer.seek(tp, int(end_offsets.get(tp, 0)))

        if logger.isEnabledFor(logging.DEBUG):
            seek_info = {
                f"{tp.topic}:{tp.partition}": {
                    "seek_offset": offset_map.get(tp).offset if offset_map.get(tp) else "end",
                    "end_offset": int(end_offsets.get(tp, 0)),
                }
                for tp in topic_partitions
            }
            logger.debug("Diaspora seek info: %s", json.dumps(seek_info, sort_keys=True))

        # Poll until caught up to end offsets or timeout.
        deadline = time.monotonic() + (max(timeout_ms, 0) / 1000.0)
        while time.monotonic() < deadline:
            remaining_ms = int(max(0.0, deadline - time.monotonic()) * 1000)
            polled = consumer.poll(timeout_ms=min(remaining_ms, 1000), max_records=max_messages)
            for partition_records in polled.values():
                for record in partition_records:
                    try:
                        value = json.loads(record.value.decode("utf-8", errors="replace"))
                    except Exception:
                        continue
                    events.append(dict(value))

            if len(events) >= max_messages:
                events = events[:max_messages]
                break

            caught_up = all(
                consumer.position(tp) >= int(end_offsets.get(tp, 0))
                for tp in topic_partitions
            )
            if caught_up:
                break
    finally:
        consumer.close()

    logger.debug(
        "Diaspora context: topic=%s time_horizon=%d events=%d",
        kafka_topic, time_horizon, len(events),
    )

    return {
        "kafka_topic": kafka_topic,
        "time_horizon": time_horizon,
        "event_count": len(events),
        "events": events,
    }
