#!/usr/bin/env python3
"""Failure demo 2: timeout failures, then succeed via retry policy.

Install dependencies from this checkout with:
    pip install -e ".[diaspora,monitoring]"

Run one-time user setup first:
    python examples/diaspora/diaspora_setup.py
"""

from __future__ import annotations

import logging
import shlex
import sys
from pathlib import Path
from typing import Any, Dict

import parsl
from parsl import AUTO_LOGNAME, bash_app
from parsl.app.app import python_app

# Allow running this script directly from bash_failures/ while importing shared helpers.
DIASPORA_DIR = Path(__file__).resolve().parent.parent
if str(DIASPORA_DIR) not in sys.path:
    sys.path.insert(0, str(DIASPORA_DIR))

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
TIMEOUT_SECONDS = 1

retry_policy_logger = logging.getLogger("parsl.examples.diaspora.failure2.retry_policy")


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
def timeout_python_once(marker_file: str, task_index: int, walltime: int = TIMEOUT_SECONDS) -> str:
    import time
    from pathlib import Path

    marker = Path(marker_file)
    marker.parent.mkdir(parents=True, exist_ok=True)
    if not marker.exists():
        marker.write_text("timeout_once\n", encoding="utf-8")
        # This should hit AppTimeout due to walltime=1.
        time.sleep(3.0)
        raise RuntimeError(f"TIMEOUT_NOT_TRIGGERED_PYTHON2 task={task_index}")
    time.sleep(0.05)
    return f"PYTHON_SUCCESS2 task={task_index} after_timeout_retry=1"


@bash_app
def timeout_bash_once(
    marker_file: str,
    task_index: int,
    stdout=AUTO_LOGNAME,
    stderr=AUTO_LOGNAME,
    walltime: int = TIMEOUT_SECONDS,
):
    marker_q = shlex.quote(marker_file)
    return "\n".join(
        [
            "set -euo pipefail",
            f'mkdir -p "$(dirname {marker_q})"',
            f"if [ ! -f {marker_q} ]; then",
            f"  touch {marker_q}",
            f'  echo "INTENTIONAL_BASH_TIMEOUT2 task={task_index}" 1>&2',
            "  sleep 3",
            "  exit 99",
            "fi",
            f'echo "BASH_SUCCESS2 task={task_index} after_timeout_retry=1"',
        ]
    )


def main(default_mode: str = "local") -> None:
    args = parse_run_args(
        description=(
            "Run timeout-based python_app + bash_app failures that recover on retry "
            "with verbose monitoring/logging."
        ),
        count_help="Number of timeout-failure task pairs.",
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
    logger = logging.getLogger(f"parsl.examples.diaspora.failure2.{args.mode}")
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
            logger.info(
                "Retry budget=%d with policy=retry_once_policy and timeout=%ds",
                RETRY_BUDGET,
                TIMEOUT_SECONDS,
            )

            marker_root = Path(dfk.run_dir) / "failure2_markers"
            marker_root.mkdir(parents=True, exist_ok=True)

            python_futures = []
            bash_futures = []
            for i in range(args.count):
                py_marker = marker_root / f"python_task_{i}.marker"
                bash_marker = marker_root / f"bash_task_{i}.marker"
                python_futures.append(
                    timeout_python_once(
                        str(py_marker),
                        i,
                        walltime=TIMEOUT_SECONDS,
                    )
                )
                bash_futures.append(
                    timeout_bash_once(
                        str(bash_marker),
                        i,
                        stdout=AUTO_LOGNAME,
                        stderr=AUTO_LOGNAME,
                        walltime=TIMEOUT_SECONDS,
                    )
                )
                logger.info("Submitted timeout failure pair %d", i)

            for i, fut in enumerate(python_futures):
                result = fut.result()
                logger.info("python_app result %d: %s", i, result)

            for i, fut in enumerate(bash_futures):
                fut.result()
                logger.info("bash_app result %d: success after timeout retry", i)

            logger.info(
                "Failure demo 2 complete: all %d python and %d bash timeout tasks succeeded after retry.",
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
