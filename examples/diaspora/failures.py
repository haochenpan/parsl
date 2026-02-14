"""Failure scenarios for the diaspora example CLI."""

from __future__ import annotations

import logging
import shlex
import sqlite3
import time
from pathlib import Path
from typing import Any

import parsl
from parsl import AUTO_LOGNAME, bash_app
from parsl.app.app import python_app
from parsl.data_provider.files import File

from runtime import execute_logged_command, make_config_for_mode

RETRY_BUDGET = 1
TASK_STREAM_TAIL_LINES = 20

FAIL_ONCE_SCENARIO = "fail-once"
TIMEOUT_SCENARIO = "timeout"
MISSING_OUTPUT_SCENARIO = "missing-output"
PYTHON_DIV_ZERO_SCENARIO = "python-div-zero"
PYTHON_CHAIN_SCENARIO = "python-chain"

SCENARIO_CHOICES = (
    FAIL_ONCE_SCENARIO,
    TIMEOUT_SCENARIO,
    MISSING_OUTPUT_SCENARIO,
    PYTHON_DIV_ZERO_SCENARIO,
    PYTHON_CHAIN_SCENARIO,
)

retry_policy_logger = logging.getLogger("parsl.examples.diaspora.failure.retry_policy")


def retry_once_policy(exception: Exception, task_record: dict[str, Any]) -> float:
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
    if fail_count <= RETRY_BUDGET:
        return 1.0
    return float(RETRY_BUDGET + 1)


def apply_debug_and_retry_policy(config: Any) -> None:
    config.retries = RETRY_BUDGET
    config.retry_handler = retry_once_policy
    if getattr(config, "monitoring", None) is not None:
        config.monitoring.monitoring_debug = True
        config.monitoring.resource_monitoring_enabled = True


def _read_tail(path: Path, line_count: int = TASK_STREAM_TAIL_LINES) -> str:
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(lines[-line_count:])


def _log_bash_stream_artifacts(logger: logging.Logger, task_index: int, future: Any) -> None:
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


def _log_monitoring_db_snapshot(logger: logging.Logger, run_id: str, config_run_dir: Path) -> None:
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


@python_app
def timeout_python_once(marker_file: str, task_index: int, walltime: int) -> str:
    import time
    from pathlib import Path

    marker = Path(marker_file)
    marker.parent.mkdir(parents=True, exist_ok=True)
    if not marker.exists():
        marker.write_text("timeout_once\n", encoding="utf-8")
        time.sleep(float(walltime) + 2.0)
        raise RuntimeError(f"TIMEOUT_NOT_TRIGGERED_PYTHON task={task_index}")
    time.sleep(0.05)
    return f"PYTHON_SUCCESS_TIMEOUT task={task_index} after_timeout_retry=1"


@bash_app
def timeout_bash_once(
    marker_file: str,
    task_index: int,
    stdout=AUTO_LOGNAME,
    stderr=AUTO_LOGNAME,
    walltime: int = 1,
):
    marker_q = shlex.quote(marker_file)
    timeout_sleep = int(walltime) + 2
    return "\n".join(
        [
            "set -euo pipefail",
            f'mkdir -p "$(dirname {marker_q})"',
            f"if [ ! -f {marker_q} ]; then",
            f"  touch {marker_q}",
            f'  echo "INTENTIONAL_BASH_TIMEOUT task={task_index}" 1>&2',
            f"  sleep {timeout_sleep}",
            "  exit 99",
            "fi",
            f'echo "BASH_SUCCESS_TIMEOUT task={task_index} after_timeout_retry=1"',
        ]
    )


@python_app
def zero_division_python_once(marker_file: str, task_index: int) -> str:
    from pathlib import Path

    marker = Path(marker_file)
    marker.parent.mkdir(parents=True, exist_ok=True)
    if not marker.exists():
        marker.write_text("zero_div_once\n", encoding="utf-8")
        _ = 1 / 0
    return f"PYTHON_SUCCESS_MISSING_OUTPUT task={task_index} after_zero_division_retry=1"


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
            f'  echo "INTENTIONAL_BASH_MISSING_OUTPUT task={task_index}" 1>&2',
            "  exit 0",
            "fi",
            f'echo "payload task={task_index}" > {output_q}',
            f'echo "BASH_SUCCESS_MISSING_OUTPUT task={task_index} after_missing_output_retry=1"',
        ]
    )


@python_app
def divide_by_zero():
    return 1 / 0


@python_app
def chain_root_fails():
    raise ValueError("Deliberate failure")


@python_app
def chain_depends(parent):
    return 1


def _run_fail_once(args, logger: logging.Logger, dfk) -> None:
    marker_root = Path(dfk.run_dir) / "failure_markers" / FAIL_ONCE_SCENARIO
    marker_root.mkdir(parents=True, exist_ok=True)

    python_futures = []
    bash_futures = []
    for index in range(args.count):
        py_marker = marker_root / f"python_task_{index}.marker"
        bash_marker = marker_root / f"bash_task_{index}.marker"
        python_future = flaky_python_once(str(py_marker), index)
        bash_future = flaky_bash_once(
            str(bash_marker),
            index,
            stdout=AUTO_LOGNAME,
            stderr=AUTO_LOGNAME,
        )
        python_futures.append(python_future)
        bash_futures.append(bash_future)
        logger.info(
            "Submitted failure pair %d: python_task_id=%s bash_task_id=%s bash_stdout=%s bash_stderr=%s",
            index,
            getattr(python_future, "tid", "n/a"),
            getattr(bash_future, "tid", "n/a"),
            bash_future.stdout,
            bash_future.stderr,
        )

    for index, fut in enumerate(python_futures):
        result = fut.result()
        logger.info("python_app result %d: %s", index, result)

    for index, fut in enumerate(bash_futures):
        fut.result()
        logger.info("bash_app result %d: success after retry", index)
        _log_bash_stream_artifacts(logger, index, fut)

    logger.info(
        "Failure scenario '%s' complete: all %d python and %d bash tasks succeeded after retry.",
        FAIL_ONCE_SCENARIO,
        len(python_futures),
        len(bash_futures),
    )


def _run_timeout(args, logger: logging.Logger, dfk) -> None:
    marker_root = Path(dfk.run_dir) / "failure_markers" / TIMEOUT_SCENARIO
    marker_root.mkdir(parents=True, exist_ok=True)

    python_futures = []
    bash_futures = []
    for index in range(args.count):
        py_marker = marker_root / f"python_task_{index}.marker"
        bash_marker = marker_root / f"bash_task_{index}.marker"
        python_futures.append(
            timeout_python_once(
                str(py_marker),
                index,
                walltime=args.timeout_seconds,
            )
        )
        bash_futures.append(
            timeout_bash_once(
                str(bash_marker),
                index,
                stdout=AUTO_LOGNAME,
                stderr=AUTO_LOGNAME,
                walltime=args.timeout_seconds,
            )
        )
        logger.info("Submitted timeout failure pair %d", index)

    for index, fut in enumerate(python_futures):
        result = fut.result()
        logger.info("python_app result %d: %s", index, result)

    for index, fut in enumerate(bash_futures):
        fut.result()
        logger.info("bash_app result %d: success after timeout retry", index)

    logger.info(
        "Failure scenario '%s' complete: all %d python and %d bash timeout tasks succeeded after retry.",
        TIMEOUT_SCENARIO,
        len(python_futures),
        len(bash_futures),
    )


def _run_missing_output(args, logger: logging.Logger, dfk) -> None:
    marker_root = Path(dfk.run_dir) / "failure_markers" / MISSING_OUTPUT_SCENARIO
    output_root = Path(dfk.run_dir) / "failure_outputs" / MISSING_OUTPUT_SCENARIO
    marker_root.mkdir(parents=True, exist_ok=True)
    output_root.mkdir(parents=True, exist_ok=True)

    python_futures = []
    bash_futures = []
    for index in range(args.count):
        py_marker = marker_root / f"python_task_{index}.marker"
        bash_marker = marker_root / f"bash_task_{index}.marker"
        bash_output = output_root / f"bash_task_{index}.txt"
        python_futures.append(zero_division_python_once(str(py_marker), index))
        bash_futures.append(
            missing_output_bash_once(
                str(bash_marker),
                str(bash_output),
                index,
                outputs=[File(str(bash_output))],
                stdout=AUTO_LOGNAME,
                stderr=AUTO_LOGNAME,
            )
        )
        logger.info("Submitted missing-output/zero-division pair %d", index)

    for index, fut in enumerate(python_futures):
        result = fut.result()
        logger.info("python_app result %d: %s", index, result)

    for index, fut in enumerate(bash_futures):
        fut.result()
        logger.info("bash_app result %d: success after missing-output retry", index)

    logger.info(
        "Failure scenario '%s' complete: all %d python and %d bash tasks succeeded after retry.",
        MISSING_OUTPUT_SCENARIO,
        len(python_futures),
        len(bash_futures),
    )


def _run_python_div_zero(args, logger: logging.Logger, _dfk) -> None:
    future = divide_by_zero()
    try:
        future.result()
        logger.error("Unexpected success")
    except Exception as exc:
        logger.info("Expected failure: %s %s", type(exc).__name__, exc)


def _run_python_chain(args, logger: logging.Logger, _dfk) -> None:
    root = chain_root_fails()
    f2 = chain_depends(root)
    f3 = chain_depends(f2)
    f4 = chain_depends(f3)

    for name, future in (("f1", root), ("f2", f2), ("f3", f3), ("f4", f4)):
        try:
            result = future.result()
            logger.info("%s success: %s", name, result)
        except Exception as exc:
            logger.info("%s failed as expected: %s %s", name, type(exc).__name__, exc)


SCENARIOS = {
    FAIL_ONCE_SCENARIO: _run_fail_once,
    TIMEOUT_SCENARIO: _run_timeout,
    MISSING_OUTPUT_SCENARIO: _run_missing_output,
    PYTHON_DIV_ZERO_SCENARIO: _run_python_div_zero,
    PYTHON_CHAIN_SCENARIO: _run_python_chain,
}


def run_failure_scenario(args) -> int:
    if args.scenario not in SCENARIOS:
        supported = ", ".join(SCENARIO_CHOICES)
        raise ValueError(f"Unknown --scenario {args.scenario!r}; supported: {supported}")

    if args.scenario in {FAIL_ONCE_SCENARIO, TIMEOUT_SCENARIO, MISSING_OUTPUT_SCENARIO} and args.count < 1:
        raise ValueError("--count must be >= 1 for this failure scenario")

    if args.timeout_seconds < 1:
        raise ValueError("--timeout-seconds must be >= 1")

    scenario_runner = SCENARIOS[args.scenario]

    def _run(context) -> None:
        logger = context.logger
        logger.info("Execution mode: %s", context.args.mode)
        logger.info("Debug logging is forced to DEBUG for this failure command.")

        config = make_config_for_mode(context.args.mode, context.args.count, logger)
        apply_debug_and_retry_policy(config)

        config_run_dir = Path(config.run_dir).resolve()
        loaded = False
        dfk_run_dir: Path | None = None
        run_id = "<not-loaded>"
        try:
            dfk = parsl.load(config)
            loaded = True
            run_id = str(getattr(dfk, "run_id", "<unknown>"))
            dfk_run_dir = Path(getattr(dfk, "run_dir", config.run_dir)).resolve()
            logger.info("Parsl loaded. run_id=%s run_dir=%s", run_id, dfk_run_dir)
            logger.info("Monitoring DB path: %s", config_run_dir / "monitoring.db")
            logger.info("Database manager log path: %s", dfk_run_dir / "database_manager.log")
            logger.info("Retry budget=%d with policy=retry_once_policy", RETRY_BUDGET)
            if args.scenario == TIMEOUT_SCENARIO:
                logger.info("Timeout scenario configured with timeout=%ds", args.timeout_seconds)

            scenario_runner(context.args, logger, dfk)
        finally:
            if loaded:
                parsl.clear()
                if args.scenario == FAIL_ONCE_SCENARIO:
                    if dfk_run_dir is not None:
                        logger.info("Task log directory for this run: %s", dfk_run_dir / "task_logs")
                    _log_monitoring_db_snapshot(logger, run_id, config_run_dir)

    scenario_logger_name = args.scenario.replace("-", "_")
    return execute_logged_command(
        args,
        logger_name=f"parsl.examples.diaspora.failure.{scenario_logger_name}.{args.mode}",
        command=_run,
        force_debug=True,
    )
