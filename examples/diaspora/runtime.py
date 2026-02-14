"""Runtime helpers shared by diaspora example commands."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import parsl

from config import (
    AURORA_MODE,
    DIASPORA_DEMO_FORMAT,
    LOCAL_MODE,
    defaults_for_mode,
    make_aurora_config,
    make_local_config,
)

DIASPORA_LOG_PREFIX = "DIASPORA|"


@dataclass(frozen=True)
class LoggedRunContext:
    args: argparse.Namespace
    logger: logging.Logger
    topic: str
    log_file: str
    log_level: int


def _log_common_workflow_start(context: LoggedRunContext, logger: logging.Logger) -> None:
    logger.info("If this is your first run, execute: python examples/diaspora/diaspora.py setup")
    logger.info("Kafka events include both raw 'message' and formatter-rendered 'formatted' fields.")
    logger.info("Execution mode: %s", context.args.mode)
    logger.info(
        "Resolved logging targets: topic=%s log_file=%s level=%s",
        context.topic,
        context.log_file,
        logging.getLevelName(context.log_level),
    )


def parse_log_level(value: str) -> int:
    level = getattr(logging, value.upper(), None)
    if not isinstance(level, int):
        raise argparse.ArgumentTypeError(f"Invalid log level: {value}")
    return level


def resolve_topic_and_log_file(
    mode: str,
    topic_override: str | None,
    log_file_override: str | None,
) -> tuple[str, str]:
    default_topic, default_log_file = defaults_for_mode(mode)
    topic = topic_override or default_topic
    log_file = log_file_override or default_log_file
    return topic, log_file


def count_file_logger_prefix_lines(log_file: str) -> int:
    matches = 0
    with Path(log_file).open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if line.startswith(DIASPORA_LOG_PREFIX):
                matches += 1
    return matches


def count_topic_messages(topic_name: str) -> tuple[str, int]:
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


def make_config_for_mode(mode: str, count: int, logger: logging.Logger) -> Any:
    account = "n/a"
    queue = "n/a"
    if mode == AURORA_MODE:
        config, account, queue = make_aurora_config(count=count)
    elif mode == LOCAL_MODE:
        config = make_local_config()
    else:
        raise ValueError(f"Unsupported mode: {mode}")

    logger.info(
        "Execution profile selected: mode=%s count=%d account=%s queue=%s",
        mode,
        count,
        account,
        queue,
    )
    return config


@contextmanager
def loaded_parsl(config: Any):
    loaded = False
    try:
        dfk = parsl.load(config)
        loaded = True
        yield dfk
    finally:
        if loaded:
            parsl.clear()


def log_post_run_metrics(logger: logging.Logger, log_file: str, topic: str) -> None:
    try:
        file_prefix_line_count = count_file_logger_prefix_lines(log_file)
        kafka_topic, topic_message_count = count_topic_messages(topic)
    except Exception:
        logger.exception("Failed to gather post-run file/topic metrics")
        return

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


def execute_logged_command(
    args: argparse.Namespace,
    logger_name: str,
    command: Callable[[LoggedRunContext], int | None],
    *,
    force_debug: bool = False,
    include_post_metrics: bool = True,
) -> int:
    log_level = logging.DEBUG if force_debug else args.log_level
    topic, log_file = resolve_topic_and_log_file(
        mode=args.mode,
        topic_override=getattr(args, "topic", None),
        log_file_override=getattr(args, "log_file", None),
    )

    close_stream_logger = parsl.set_stream_logger(level=log_level)
    close_file_logger = parsl.set_file_logger(
        log_file,
        level=log_level,
        format_string=DIASPORA_DEMO_FORMAT,
    )
    close_diaspora_logger = parsl.set_diaspora_logger(
        topic_name=topic,
        name="parsl",
        level=log_level,
        format_string=DIASPORA_DEMO_FORMAT,
    )
    logger = logging.getLogger(logger_name)
    context = LoggedRunContext(
        args=args,
        logger=logger,
        topic=topic,
        log_file=log_file,
        log_level=log_level,
    )

    result = 0
    try:
        command_result = command(context)
        if command_result is not None:
            result = int(command_result)
    finally:
        try:
            close_diaspora_logger()
        except Exception:
            logger.exception("Failed to close Diaspora logger")
        try:
            close_file_logger()
        except Exception:
            logger.exception("Failed to close file logger")

        if include_post_metrics:
            log_post_run_metrics(logger, log_file, topic)

        try:
            close_stream_logger()
        except Exception:
            logger.exception("Failed to close stream logger")

    return result
