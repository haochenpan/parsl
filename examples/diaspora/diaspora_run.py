#!/usr/bin/env python3
"""Minimal Parsl hello-world workflow with Diaspora event logging.

Install dependencies from this checkout with:
    pip install -e ".[diaspora]"

Run one-time user setup first:
    python examples/diaspora/diaspora_setup.py
"""

from __future__ import annotations

import logging

import parsl
from parsl.app.app import python_app

from config import (
    AURORA_MODE,
    DIASPORA_DEMO_FORMAT,
    make_aurora_config,
    make_local_config,
    parse_run_args,
)
from util import (
    count_file_logger_prefix_lines,
    count_topic_messages,
    resolve_topic_and_log_file,
)


def main(default_mode: str = "local") -> None:
    args = parse_run_args(
        description="Run a hello-world Parsl app and send log events to Diaspora.",
        count_help="Number of hello tasks.",
        default_mode=default_mode,
    )
    is_aurora = args.mode == AURORA_MODE
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
