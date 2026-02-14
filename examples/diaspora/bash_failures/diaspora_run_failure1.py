#!/usr/bin/env python3
"""Failure demo 1: fail once, then succeed via retry policy.

Install dependencies from this checkout with:
    pip install -e ".[diaspora,monitoring]"

Run one-time user setup first:
    python examples/diaspora/diaspora_setup.py
"""

from __future__ import annotations

import logging
import shlex
import sqlite3
import sys
import time
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
TASK_STREAM_TAIL_LINES = 20

retry_policy_logger = logging.getLogger("parsl.examples.diaspora.failure1.retry_policy")


def retry_once_policy(exception: Exception, task_record: Dict[str, Any]) -> float:
    """Allow one retry per task for this demo."""
    fail_count = int(task_record.get("fail_count", 0))
    task_id = task_record.get("id", "unknown")
    try_id = task_record.get("try_id", "unknown")
    fail_history = task_record.get("fail_history", [])
    retry_policy_logger.warning(
        "Retry policy observed task_id=%s try_id=%s fail_count=%s exception_type=%s exception=%r fail_history=%s",
        task_id,
        try_id,
        fail_count,
        type(exception).__name__,
        exception,
        fail_history,
    )
    if hasattr(exception, "exitcode"):
        retry_policy_logger.warning(
            "Retry policy extra detail task_id=%s app_name=%s exitcode=%s",
            task_id,
            getattr(exception, "app_name", "<unknown>"),
            getattr(exception, "exitcode", "<unknown>"),
        )
    if fail_count <= 1:
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
def flaky_python_once(marker_file: str, task_index: int) -> str:
    from pathlib import Path

    marker = Path(marker_file)
    marker.parent.mkdir(parents=True, exist_ok=True)
    if not marker.exists():
        marker.write_text("failed_once\n", encoding="utf-8")
        raise RuntimeError(f"INTENTIONAL_PYTHON_FAILURE1 task={task_index}")
    return f"PYTHON_SUCCESS1 task={task_index}"


@bash_app
def flaky_bash_once(
    marker_file: str,
    task_index: int,
    stdout=AUTO_LOGNAME,
    stderr=AUTO_LOGNAME,
):
    marker_q = shlex.quote(marker_file)
    return "\n".join(
        [
            "set -euo pipefail",
            f'mkdir -p "$(dirname {marker_q})"',
            f"if [ ! -f {marker_q} ]; then",
            f'  echo "INTENTIONAL_BASH_FAILURE1 task={task_index}" 1>&2',
            f"  touch {marker_q}",
            "  exit 23",
            "fi",
            f'echo "BASH_SUCCESS1 task={task_index}"',
        ]
    )


def _read_tail(path: Path, line_count: int = TASK_STREAM_TAIL_LINES) -> str:
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(lines[-line_count:])


def log_bash_stream_artifacts(logger: logging.Logger, task_index: int, future: Any) -> None:
    task_id = getattr(future, "tid", "unknown")
    stdout_target = future.stdout
    stderr_target = future.stderr
    logger.info(
        "bash_app diagnostics task_index=%s task_id=%s stdout=%s stderr=%s",
        task_index,
        task_id,
        stdout_target,
        stderr_target,
    )
    for stream_name, target in (("stdout", stdout_target), ("stderr", stderr_target)):
        if not isinstance(target, str):
            continue
        path = Path(target)
        if not path.exists():
            logger.warning(
                "bash_app %s file missing task_index=%s task_id=%s path=%s",
                stream_name,
                task_index,
                task_id,
                path,
            )
            continue
        tail = _read_tail(path)
        if tail:
            logger.info(
                "bash_app %s tail task_index=%s task_id=%s path=%s\n%s",
                stream_name,
                task_index,
                task_id,
                path,
                tail,
            )


def log_monitoring_db_snapshot(logger: logging.Logger, run_id: str, config_run_dir: Path) -> None:
    db_path = config_run_dir / "monitoring.db"
    deadline = time.time() + 8.0
    while time.time() < deadline and not db_path.exists():
        time.sleep(0.2)

    if not db_path.exists():
        logger.warning("monitoring.db not found at %s", db_path)
        return

    query_deadline = time.time() + 10.0
    while True:
        try:
            with sqlite3.connect(str(db_path)) as conn:
                try_rows = conn.execute(
                    "SELECT task_id, try_id, task_executor, task_fail_history "
                    "FROM try WHERE run_id = ? ORDER BY task_id, try_id",
                    (run_id,),
                ).fetchall()
                task_rows = conn.execute(
                    "SELECT task_id, task_func_name, task_stdout, task_stderr "
                    "FROM task WHERE run_id = ? ORDER BY task_id",
                    (run_id,),
                ).fetchall()
            if try_rows or task_rows or time.time() >= query_deadline:
                break
            time.sleep(0.2)
        except sqlite3.OperationalError:
            if time.time() >= query_deadline:
                logger.exception("Failed to query monitoring.db at %s for run_id=%s", db_path, run_id)
                return
            time.sleep(0.2)
        except Exception:
            logger.exception("Failed to query monitoring.db at %s for run_id=%s", db_path, run_id)
            return

    logger.info(
        "monitoring.db snapshot run_id=%s path=%s try_rows=%d task_rows=%d",
        run_id,
        db_path,
        len(try_rows),
        len(task_rows),
    )
    for task_id, try_id, task_executor, task_fail_history in try_rows:
        if task_fail_history:
            logger.warning(
                "monitoring.try failure row task_id=%s try_id=%s executor=%s fail_history=%s",
                task_id,
                try_id,
                task_executor,
                task_fail_history,
            )
    for task_id, task_func_name, task_stdout, task_stderr in task_rows:
        if task_stdout or task_stderr:
            logger.info(
                "monitoring.task stream row task_id=%s func=%s stdout=%s stderr=%s",
                task_id,
                task_func_name,
                task_stdout,
                task_stderr,
            )


def main(default_mode: str = "local") -> None:
    args = parse_run_args(
        description=(
            "Run artificial python_app + bash_app failures that recover on retry "
            "with verbose monitoring/logging."
        ),
        count_help="Number of failing task pairs.",
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
    logger = logging.getLogger(f"parsl.examples.diaspora.failure1.{args.mode}")
    loaded = False
    config_run_dir: Path | None = None
    dfk_run_dir: Path | None = None
    run_id = "<not-loaded>"

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

            config_run_dir = Path(config.run_dir).resolve()
            apply_debug_and_retry_policy(config)
            dfk = parsl.load(config)
            loaded = True
            run_id = str(getattr(dfk, "run_id", "<unknown>"))
            dfk_run_dir = Path(getattr(dfk, "run_dir", config.run_dir)).resolve()
            logger.info("Parsl loaded. run_id=%s run_dir=%s", run_id, dfk_run_dir)
            logger.info("Monitoring DB path: %s", config_run_dir / "monitoring.db")
            logger.info("Database manager log path: %s", dfk_run_dir / "database_manager.log")
            logger.info("Retry budget=%d with policy=retry_once_policy", RETRY_BUDGET)

            marker_root = Path(dfk.run_dir) / "failure1_markers"
            marker_root.mkdir(parents=True, exist_ok=True)

            python_futures = []
            bash_futures = []
            for i in range(args.count):
                py_marker = marker_root / f"python_task_{i}.marker"
                bash_marker = marker_root / f"bash_task_{i}.marker"
                python_future = flaky_python_once(str(py_marker), i)
                bash_future = flaky_bash_once(
                    str(bash_marker),
                    i,
                    stdout=AUTO_LOGNAME,
                    stderr=AUTO_LOGNAME,
                )
                python_futures.append(python_future)
                bash_futures.append(bash_future)
                logger.info(
                    "Submitted failure pair %d: python_task_id=%s bash_task_id=%s bash_stdout=%s bash_stderr=%s",
                    i,
                    getattr(python_future, "tid", "n/a"),
                    getattr(bash_future, "tid", "n/a"),
                    bash_future.stdout,
                    bash_future.stderr,
                )

            for i, fut in enumerate(python_futures):
                result = fut.result()
                logger.info("python_app result %d: %s", i, result)

            for i, fut in enumerate(bash_futures):
                fut.result()
                logger.info("bash_app result %d: success after retry", i)
                log_bash_stream_artifacts(logger, i, fut)

            logger.info(
                "Failure demo 1 complete: all %d python and %d bash tasks succeeded after retry.",
                len(python_futures),
                len(bash_futures),
            )
        finally:
            if loaded:
                parsl.clear()
                if dfk_run_dir is not None:
                    logger.info("Task log directory for this run: %s", dfk_run_dir / "task_logs")
                if config_run_dir is not None:
                    log_monitoring_db_snapshot(logger, run_id, config_run_dir)
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
