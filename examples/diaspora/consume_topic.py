#!/usr/bin/env python3
"""Consume Diaspora topic events and print records containing 'python_app'.

Usage:
    python examples/diaspora/consume_topic.py [--mode MODE] [--topic TOPIC]
"""

from __future__ import annotations

import argparse
import json
from typing import Any

from diaspora_event_sdk import Client
from diaspora_event_sdk import KafkaConsumer

from config import AURORA_MODE, LOCAL_MODE, defaults_for_mode

FILTER_TEXT = "python_app"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=[LOCAL_MODE, AURORA_MODE],
        default=LOCAL_MODE,
        help="Default topic profile to use when --topic is omitted.",
    )
    parser.add_argument("--topic", default=None, help="Diaspora topic name without namespace.")
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=10000,
        help="Consumer timeout in milliseconds when reading existing records.",
    )
    return parser.parse_args()


def to_text(payload: Any) -> str:
    if payload is None:
        return ""
    if isinstance(payload, bytes):
        return payload.decode("utf-8", errors="replace")
    if isinstance(payload, str):
        return payload
    if isinstance(payload, (dict, list)):
        return json.dumps(payload, sort_keys=True, default=str)
    return str(payload)


def main() -> int:
    args = parse_args()
    default_topic, _ = defaults_for_mode(args.mode)
    topic = args.topic or default_topic

    client = Client()
    kafka_topic = f"{client.namespace}.{topic}"

    consumer = KafkaConsumer(
        kafka_topic,
        auto_offset_reset="earliest",
        consumer_timeout_ms=args.timeout_ms,
        enable_auto_commit=False,
    )

    matches = 0
    try:
        for record in consumer:
            value = getattr(record, "value", record)
            text = to_text(value)
            if FILTER_TEXT not in text:
                continue

            matches += 1
            if isinstance(value, (dict, list)):
                printable = json.dumps(value, sort_keys=True, default=str)
            else:
                printable = text
            print(printable)
    finally:
        consumer.close()

    print(f"Matched {matches} event(s) containing {FILTER_TEXT!r} in topic '{kafka_topic}'.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
