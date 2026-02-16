from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, List, Mapping, Optional

logger = logging.getLogger(__name__)


def _task_matches(event: Mapping[str, Any], task_id: int) -> bool:
    explicit_ids = [event.get("parsl_task_id"), event.get("task_id")]
    for candidate in explicit_ids:
        if candidate is None:
            continue
        try:
            if int(candidate) == task_id:
                return True
        except Exception:
            continue

    joined = " ".join(str(event.get(k, "")) for k in ("message", "formatted"))
    return f"Task {task_id}" in joined or f"task {task_id}" in joined


def _run_matches(event: Mapping[str, Any], run_id: str) -> bool:
    candidates = [event.get("parsl_run_id"), event.get("run_id")]
    for candidate in candidates:
        if candidate is None:
            continue
        if str(candidate) == run_id:
            return True

    joined = " ".join(str(event.get(k, "")) for k in ("message", "formatted"))
    return run_id in joined


def _retry_relevant(event: Mapping[str, Any]) -> bool:
    name = str(event.get("name", ""))
    message = " ".join(str(event.get(k, "")) for k in ("message", "formatted"))
    message_lower = message.lower()
    needle_hits = any(token in message_lower for token in ("retry", "exception", "syntaxerror")) or "Traceback" in message
    return needle_hits or name.startswith("parsl")


def _collect_tail_records(consumer: Any, *, max_messages: int, timeout_ms: int) -> List[Any]:
    """Collect at most max_messages near the latest offsets without waiting for new messages."""
    if max_messages <= 0:
        return []

    topic_partitions = []
    for _ in range(3):
        consumer.poll(timeout_ms=min(timeout_ms, 1000))
        topic_partitions = list(consumer.assignment())
        if topic_partitions:
            break
    if not topic_partitions:
        return []

    end_offsets = consumer.end_offsets(topic_partitions)
    beginning_offsets = consumer.beginning_offsets(topic_partitions)
    found_offsets: Dict[str, Dict[str, int]] = {}
    for partition in topic_partitions:
        found_offsets[f"{partition.topic}:{partition.partition}"] = {
            "beginning": int(beginning_offsets.get(partition, 0)),
            "end": int(end_offsets.get(partition, 0)),
        }
    logger.info("Diaspora offsets found: %s", json.dumps(found_offsets, sort_keys=True))

    for partition in topic_partitions:
        begin = int(beginning_offsets.get(partition, 0))
        end = int(end_offsets.get(partition, 0))
        start = max(begin, end - max_messages)
        consumer.seek(partition, start)

    records: List[Any] = []
    deadline = time.monotonic() + (max(timeout_ms, 0) / 1000.0)
    empty_polls = 0

    while True:
        remaining_ms = int(max(0.0, deadline - time.monotonic()) * 1000)
        poll_timeout_ms = min(200, remaining_ms) if remaining_ms > 0 else 0

        polled = consumer.poll(timeout_ms=poll_timeout_ms, max_records=max_messages)
        if not polled:
            empty_polls += 1
        else:
            empty_polls = 0
            for partition_records in polled.values():
                records.extend(partition_records)

        caught_up = True
        for partition in topic_partitions:
            try:
                if consumer.position(partition) < int(end_offsets.get(partition, 0)):
                    caught_up = False
                    break
            except Exception:
                caught_up = False
                break

        if caught_up:
            break
        if empty_polls >= 3:
            break
        if time.monotonic() >= deadline:
            break

    records.sort(
        key=lambda record: (
            int(getattr(record, "timestamp", -1) or -1),
            int(getattr(record, "partition", -1) or -1),
            int(getattr(record, "offset", -1) or -1),
        )
    )
    if len(records) > max_messages:
        records = records[-max_messages:]

    return records


def fetch_diaspora_context(
    *,
    topic_name: str,
    run_id: Optional[str] = None,
    timeout_ms: int = 30000,
    max_messages: int = 100,
    environment: Optional[str] = None,
) -> Dict[str, Any]:
    """Fetch and normalize retry context from a Diaspora topic."""

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
        for record in tail_records:
            try:
                value = json.loads(record.value.decode("utf-8", errors="replace"))
            except Exception:
                continue
            event = dict(value)

            if run_id is not None:
                name = str(event.get("name", ""))
                if not name.startswith("parsl.examples."):
                    continue
                if str(event.get("run_id", "")) != run_id:
                    continue

            scanned += 1
            matched.append(event.get("message"))

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
