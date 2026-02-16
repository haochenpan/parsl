"""Workflow implementations for diaspora example commands."""

from __future__ import annotations

import time

from parsl.app.app import python_app

from config import (
    AURORA_MODE,
    AURORA_MONITORING_INTERVAL_SECONDS,
    LOCAL_MONITORING_INTERVAL_SECONDS,
)
from runtime import _log_common_workflow_start, execute_logged_command, loaded_parsl, make_config_for_mode

PI_REFERENCE = 3.141592653589793
MONTE_CARLO_POINTS_PER_WORKER = 2_000_000


def _workflow_log_extra(*, run_id=None, task_id=None) -> dict[str, object]:
    extra: dict[str, object] = {}
    if run_id is not None:
        extra["run_id"] = run_id
    if task_id is not None:
        extra["task_id"] = task_id
    return extra


def _log_info(logger, message: str, *args, run_id=None, task_id=None) -> None:
    logger.info(
        message,
        *args,
        extra=_workflow_log_extra(run_id=run_id, task_id=task_id),
    )


def run_hello_workflow(args) -> int:
    if args.count < 1:
        raise ValueError("--count must be >= 1")

    def _run(context) -> None:
        logger = context.logger
        run_id = None

        def log_info(message: str, *args, task_id=None) -> None:
            _log_info(logger, message, *args, run_id=run_id, task_id=task_id)

        _log_common_workflow_start(context, logger)
        log_info("Launching hello workflow with %d tasks.", context.args.count)

        task_env = "Aurora" if context.args.mode == AURORA_MODE else "local"
        config = make_config_for_mode(context.args.mode, context.args.count, logger)

        with loaded_parsl(config) as dfk:
            run_id = getattr(dfk, "run_id", None)
            log_info(
                "Parsl loaded. run_id=%s",
                run_id if run_id is not None else "<unknown>",
            )

            @python_app
            def hello(i: int) -> str:
                return f"Hello from Parsl {task_env} task {i}"

            futures = [hello(i) for i in range(context.args.count)]
            for future in futures:
                task_id = getattr(future, "tid", None)
                result = future.result()
                log_info(
                    "Task result: %s",
                    result,
                    task_id=task_id,
                )

        log_info(
            "Completed %d tasks. Log records were sent to Diaspora topic '%s'.",
            context.args.count,
            context.topic,
        )

    return execute_logged_command(
        args,
        logger_name="parsl.examples.diaspora.hello",
        command=_run,
    )


def run_monte_carlo_workflow(args) -> int:
    if args.count < 1:
        raise ValueError("--count must be >= 1")

    def _run(context) -> None:
        logger = context.logger
        is_aurora = context.args.mode == AURORA_MODE
        workflow_start = time.time()
        run_id = None

        def log_info(message: str, *args, task_id=None) -> None:
            _log_info(logger, message, *args, run_id=run_id, task_id=task_id)

        _log_common_workflow_start(context, logger)
        log_info(
            "Launching Monte Carlo Pi with %d workers and %d points per worker.",
            context.args.count,
            MONTE_CARLO_POINTS_PER_WORKER,
        )

        config = make_config_for_mode(context.args.mode, context.args.count, logger)
        with loaded_parsl(config) as dfk:
            run_id = getattr(dfk, "run_id", None)
            log_info(
                "Parsl loaded. run_id=%s",
                run_id if run_id is not None else "<unknown>",
            )

            @python_app
            def estimate_pi_worker(
                worker_id: int,
                sample_count: int,
            ) -> dict:
                import random
                import time as _time

                worker_logs = []
                start_msg = f"[python_app] worker {worker_id} starting sample_count={sample_count}"
                worker_logs.append(start_msg)
                log_info(start_msg)
                rng = random.Random(9100 + worker_id)
                inside = 0
                start = _time.time()
                for _ in range(sample_count):
                    x = rng.random()
                    y = rng.random()
                    if (x * x) + (y * y) <= 1.0:
                        inside += 1
                end = _time.time()
                partial_pi = 4.0 * inside / sample_count
                finish_msg = (
                    f"[python_app] worker {worker_id} finished in {end - start:.2f}s "
                    f"inside={inside} partial_pi={partial_pi:.8f}"
                )
                worker_logs.append(finish_msg)
                log_info(finish_msg)

                return {
                    "worker_id": worker_id,
                    "samples": sample_count,
                    "inside": inside,
                    "pi_partial": partial_pi,
                    "duration_s": end - start,
                    "worker_logs": worker_logs,
                }

            @python_app
            def aggregate_pi(*worker_summaries: dict) -> dict:
                summaries = list(worker_summaries)
                total_samples = sum(item["samples"] for item in summaries)
                total_inside = sum(item["inside"] for item in summaries)
                pi_estimate = 4.0 * total_inside / total_samples
                abs_error = abs(pi_estimate - PI_REFERENCE)
                return {
                    "worker_count": len(summaries),
                    "total_samples": total_samples,
                    "total_inside": total_inside,
                    "pi_estimate": pi_estimate,
                    "absolute_error": abs_error,
                    "best_partial_worker": min(
                        summaries,
                        key=lambda x: abs(x["pi_partial"] - PI_REFERENCE),
                    )["worker_id"],
                }

            monte_carlo_start = time.time()
            worker_futures = []
            for i in range(context.args.count):
                future = estimate_pi_worker(i, MONTE_CARLO_POINTS_PER_WORKER)
                worker_futures.append(future)
                task_id = getattr(future, "tid", None)
                log_info(
                    "Submitted worker %d (task_id=%s)",
                    i,
                    task_id if task_id is not None else "n/a",
                    task_id=task_id,
                )
            aggregate_future = aggregate_pi(*worker_futures)
            aggregate_task_id = getattr(aggregate_future, "tid", None)
            log_info(
                "Submitted final aggregation worker (task_id=%s) waiting on %d workers",
                aggregate_task_id if aggregate_task_id is not None else "n/a",
                len(worker_futures),
                task_id=aggregate_task_id,
            )

            pending = {f: i for i, f in enumerate(worker_futures)}
            worker_results: dict[int, dict] = {}
            heartbeat_seconds = (
                AURORA_MONITORING_INTERVAL_SECONDS
                if is_aurora
                else LOCAL_MONITORING_INTERVAL_SECONDS
            )
            log_info(
                "Heartbeat interval: %.1fs",
                heartbeat_seconds,
            )
            while pending:
                completed_now = [f for f in pending if f.done()]
                for future in completed_now:
                    worker_id = pending.pop(future)
                    task_id = getattr(future, "tid", None)
                    result = future.result()
                    worker_results[worker_id] = result
                    if is_aurora:
                        # Re-emit worker logs on submit side through the Diaspora logger.
                        for worker_log in result.get("worker_logs", []):
                            log_info(
                                "%s",
                                worker_log,
                                task_id=task_id,
                            )
                    log_info(
                        "Worker %d complete: inside=%d/%d partial_pi=%.8f duration=%.2fs",
                        worker_id,
                        result["inside"],
                        result["samples"],
                        result["pi_partial"],
                        result["duration_s"],
                        task_id=task_id,
                    )

                elapsed = time.time() - monte_carlo_start
                pending_ids = sorted(pending.values())
                log_info(
                    "Monte Carlo heartbeat: elapsed=%.1fs completed=%d/%d pending=%d pending_ids=%s final_worker_done=%s",
                    elapsed,
                    context.args.count - len(pending),
                    context.args.count,
                    len(pending),
                    pending_ids,
                    aggregate_future.done(),
                    task_id=aggregate_task_id,
                )
                if pending:
                    time.sleep(heartbeat_seconds)

            log_info(
                "All worker estimates finished; waiting for final aggregation worker.",
                task_id=aggregate_task_id,
            )
            summary = aggregate_future.result()
            total_monte_carlo_s = time.time() - monte_carlo_start
            log_info(
                "Final Pi estimate=%.10f absolute_error=%.10f using workers=%d total_samples=%d total_inside=%d",
                summary["pi_estimate"],
                summary["absolute_error"],
                summary["worker_count"],
                summary["total_samples"],
                summary["total_inside"],
                task_id=aggregate_task_id,
            )
            log_info(
                "Best partial worker by absolute error: worker_id=%d",
                summary["best_partial_worker"],
                task_id=aggregate_task_id,
            )
            log_info(
                "Completed Monte Carlo Pi workflow with %d workers in %.1fs. Log records were sent to Diaspora topic '%s'.",
                context.args.count,
                total_monte_carlo_s,
                context.topic,
                task_id=aggregate_task_id,
            )

        log_info(
            "Workflow wall-clock time: %.1fs",
            time.time() - workflow_start,
        )

    return execute_logged_command(
        args,
        logger_name="parsl.examples.diaspora.monte",
        command=_run,
    )
