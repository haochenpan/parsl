"""Helper functions shared by diaspora example scripts."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from config import defaults_for_mode

DIASPORA_LOG_PREFIX = "DIASPORA|"


def count_file_logger_prefix_lines(log_file: str) -> int:
    """Count file log lines emitted by this script's file logger format."""
    matches = 0
    with Path(log_file).open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if line.startswith(DIASPORA_LOG_PREFIX):
                matches += 1
    return matches


def count_topic_messages(topic_name: str) -> tuple[str, int]:
    """Count all currently readable messages in namespace.topic using Diaspora KafkaConsumer."""
    from diaspora_event_sdk import Client as GlobusClient
    from diaspora_event_sdk import KafkaConsumer

    client = GlobusClient()
    kafka_topic = f"{client.namespace}.{topic_name}"
    consumer = KafkaConsumer(
        kafka_topic,
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,
        enable_auto_commit=False,
    )
    try:
        message_count = sum(1 for _ in consumer)
    finally:
        consumer.close()
    return kafka_topic, message_count


def resolve_topic_and_log_file(
    mode: str,
    topic_override: str | None,
    log_file_override: str | None,
) -> tuple[str, str]:
    default_topic, default_log_file = defaults_for_mode(mode)
    topic = topic_override or default_topic
    log_file = log_file_override or default_log_file
    return topic, log_file


def parse_log_level(value: str) -> int:
    level = getattr(logging, value.upper(), None)
    if not isinstance(level, int):
        raise argparse.ArgumentTypeError(f"Invalid log level: {value}")
    return level
