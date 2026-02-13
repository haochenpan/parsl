#!/usr/bin/env python3
"""Failure demo 3: missing outputs + logic exceptions, then succeed via retry.

Install dependencies from this checkout with:
    pip install -e ".[diaspora,monitoring]"

Run one-time user setup first:
    python examples/diaspora/diaspora_setup.py
"""

from __future__ import annotations

import logging
import shlex
from pathlib import Path
from typing import Any, Dict

import parsl
from parsl import AUTO_LOGNAME, bash_app
from parsl.app.app import python_app
from parsl.data_provider.files import File

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

RETRY_BUDGET = 1

retry_policy_logger = logging.getLogger("parsl.examples.diaspora.failure3.retry_policy")


def retry_once_policy(exception: Exception, task_record: Dict[str, Any]) -> float:
    """Allow one retry per task for this demo."""
    fail_count = int(task_record.get("fail_count", 0))
    task_id = task_record.get("id", "unknown")
    retry_policy_logger.warning(
        "Retry policy observed task_id=%s fail_count=%s exception_type=%s",
        task_id,
        fail_count,
        type(exception).__name__,
    )
    if fail_count <= RETRY_BUDGET:
        return 1.0
    return float(RETRY_BUDGET + 1)


def apply_debug_and_retry_policy(config: Any) -> None:
    """Turn on verbose monitoring/logging and set retry behavior."""
    config.retries = RETRY_BUDGET
    config.retry_handler = retry_once_policy
    if getattr(config, "monitoring", None) is not None:
        config.monitoring.monitoring_debug = True
        config.monitoring.resource_monitoring_enabled = True


@python_app
def zero_division_python_once(marker_file: str, task_index: int) -> str:
    from pathlib import Path

    marker = Path(marker_file)
    marker.parent.mkdir(parents=True, exist_ok=True)
    if not marker.exists():
        marker.write_text("zero_div_once\n", encoding="utf-8")
        _ = 1 / 0
    return f"PYTHON_SUCCESS3 task={task_index} after_zero_division_retry=1"


@bash_app
def missing_output_bash_once(
    marker_file: str,
    output_file: str,
    task_index: int,
    outputs=(),
    stdout=AUTO_LOGNAME,
    stderr=AUTO_LOGNAME,
):
    marker_q = shlex.quote(marker_file)
    output_q = shlex.quote(output_file)
    return "\n".join(
        [
            "set -euo pipefail",
            f'mkdir -p "$(dirname {marker_q})"',
            f'mkdir -p "$(dirname {output_q})"',
            f"if [ ! -f {marker_q} ]; then",
            f"  touch {marker_q}",
            f'  echo "INTENTIONAL_BASH_MISSING_OUTPUT3 task={task_index}" 1>&2',
            "  # Exit 0 but do not create declared output to trigger MissingOutputs.",
            "  exit 0",
            "fi",
            f'echo "payload task={task_index}" > {output_q}',
            f'echo "BASH_SUCCESS3 task={task_index} after_missing_output_retry=1"',
        ]
    )


def main(default_mode: str = "local") -> None:
    args = parse_run_args(
        description=(
            "Run zero-division + missing-output failures that recover on retry "
            "with verbose monitoring/logging."
        ),
        count_help="Number of missing-output/zero-division task pairs.",
        default_mode=default_mode,
    )
    args.log_level = logging.DEBUG
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
    logger = logging.getLogger(f"parsl.examples.diaspora.failure3.{args.mode}")
    loaded = False

    try:
        try:
            logger.info("Execution mode: %s", args.mode)
            logger.info("Debug logging is forced to DEBUG for this failure demo.")
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

            apply_debug_and_retry_policy(config)
            dfk = parsl.load(config)
            loaded = True
            logger.info("Parsl loaded. run_id=%s", getattr(dfk, "run_id", "<unknown>"))
            logger.info("Retry budget=%d with policy=retry_once_policy", RETRY_BUDGET)

            marker_root = Path(dfk.run_dir) / "failure3_markers"
            output_root = Path(dfk.run_dir) / "failure3_outputs"
            marker_root.mkdir(parents=True, exist_ok=True)
            output_root.mkdir(parents=True, exist_ok=True)

            python_futures = []
            bash_futures = []
            for i in range(args.count):
                py_marker = marker_root / f"python_task_{i}.marker"
                bash_marker = marker_root / f"bash_task_{i}.marker"
                bash_output = output_root / f"bash_task_{i}.txt"
                python_futures.append(zero_division_python_once(str(py_marker), i))
                bash_futures.append(
                    missing_output_bash_once(
                        str(bash_marker),
                        str(bash_output),
                        i,
                        outputs=[File(str(bash_output))],
                        stdout=AUTO_LOGNAME,
                        stderr=AUTO_LOGNAME,
                    )
                )
                logger.info("Submitted missing-output/zero-division pair %d", i)

            for i, fut in enumerate(python_futures):
                result = fut.result()
                logger.info("python_app result %d: %s", i, result)

            for i, fut in enumerate(bash_futures):
                fut.result()
                logger.info("bash_app result %d: success after missing-output retry", i)

            logger.info(
                "Failure demo 3 complete: all %d python and %d bash tasks succeeded after retry.",
                len(python_futures),
                len(bash_futures),
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
