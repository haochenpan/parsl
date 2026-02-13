#!/usr/bin/env python3
"""Minimal Parsl hello-world workflow with Diaspora event logging.

Install dependencies from this checkout with:
    pip install -e ".[diaspora]"

Run one-time user setup first:
    python examples/diaspora/diaspora_setup.py
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import parsl
from parsl.app.app import python_app

from config import (
    AURORA_MODE,
    LOCAL_MODE,
    defaults_for_mode,
    make_aurora_config,
    make_local_config,
)

DIASPORA_DEMO_FORMAT = "DIASPORA|%(levelname)s|%(name)s|%(funcName)s:%(lineno)d|%(message)s"
DIASPORA_LOG_PREFIX = "DIASPORA|"


def parse_log_level(value: str) -> int:
    level = getattr(logging, value.upper(), None)
    if not isinstance(level, int):
        raise argparse.ArgumentTypeError(f"Invalid log level: {value}")
    return level


def parse_args(default_mode: str = "local") -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a hello-world Parsl app and send log events to Diaspora.")
    parser.add_argument(
        "--mode",
        choices=[LOCAL_MODE, AURORA_MODE],
        default=default_mode,
        help="Execution mode: local thread pool or Aurora PBS.",
    )
    parser.add_argument("--topic", default=None, help="Diaspora topic name (without namespace).")
    parser.add_argument("--count", type=int, default=3, help="Number of hello tasks.")
    parser.add_argument("--log-file", default=None, help="Local file logger output path.")
    parser.add_argument(
        "--log-level",
        type=parse_log_level,
        default=logging.INFO,
        help="Logging level for stream/file/Diaspora handlers (for example: DEBUG, INFO, WARNING).",
    )
    return parser.parse_args()


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


def main(default_mode: str = "local") -> None:
    args = parse_args(default_mode=default_mode)
    is_aurora = args.mode == "aurora"
    topic, log_file = resolve_topic_and_log_file(
        mode=args.mode,
        topic_override=args.topic,
        log_file_override=args.log_file,
    )
    close_stream_logger = parsl.set_stream_logger(level=args.log_level)
    close_file_logger = parsl.set_file_logger(
        log_file,
        level=args.log_level,
        format_string=DIASPORA_DEMO_FORMAT,
    )
    close_diaspora_logger = parsl.set_diaspora_logger(
        topic_name=topic,
        name="parsl",
        level=args.log_level,
        format_string=DIASPORA_DEMO_FORMAT,
    )
    logger = logging.getLogger(f"parsl.examples.diaspora.{args.mode}")
    loaded = False

    try:
        try:
            logger.info("If this is your first run, execute: python examples/diaspora/diaspora_setup.py")
            logger.info("Kafka events include both raw 'message' and formatter-rendered 'formatted' fields.")
            logger.info("Execution mode: %s", args.mode)

            task_env = "Aurora" if is_aurora else "local"
            if is_aurora:
                config, account, queue = make_aurora_config(count=args.count)
                logger.info(
                    "Aurora profile selected for count=%d: account=%s queue=%s",
                    args.count,
                    account,
                    queue,
                )
            else:
                config = make_local_config()
            parsl.load(config)
            loaded = True

            @python_app
            def hello(i: int) -> str:
                return f"Hello from Parsl {task_env} task {i}"

            futures = [hello(i) for i in range(args.count)]
            results = [f.result() for f in futures]
            for result in results:
                logger.info("Task result: %s", result)

            logger.info(
                "Completed %d tasks. Log records were sent to Diaspora topic '%s'.",
                args.count,
                topic,
            )
        finally:
            if loaded:
                parsl.clear()
            close_diaspora_logger()
            close_file_logger()

        file_prefix_line_count = count_file_logger_prefix_lines(log_file)
        kafka_topic, topic_message_count = count_topic_messages(topic)

        logger.info(
            "File logger prefix line count in '%s': %d",
            log_file,
            file_prefix_line_count,
        )
        logger.info(
            "Kafka message count in topic '%s': %d",
            kafka_topic,
            topic_message_count,
        )
    finally:
        close_stream_logger()


if __name__ == "__main__":
    main()
