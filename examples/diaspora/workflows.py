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


def run_hello_workflow(args) -> int:
    if args.count < 1:
        raise ValueError("--count must be >= 1")

    def _run(context) -> None:
        logger = context.logger

        _log_common_workflow_start(context, logger)
        logger.info("Launching hello workflow with %d tasks.", context.args.count)

        task_env = "Aurora" if context.args.mode == AURORA_MODE else "local"
        config = make_config_for_mode(context.args.mode, context.args.count, logger)

        with loaded_parsl(config) as dfk:
            logger.info("Parsl loaded. run_id=%s", getattr(dfk, "run_id", "<unknown>"))

            @python_app
            def hello(i: int) -> str:
                return f"Hello from Parsl {task_env} task {i}"

            futures = [hello(i) for i in range(context.args.count)]
            results = [f.result() for f in futures]
            for result in results:
                logger.info("Task result: %s", result)

        logger.info(
            "Completed %d tasks. Log records were sent to Diaspora topic '%s'.",
            context.args.count,
            context.topic,
        )

    return execute_logged_command(
        args,
        logger_name=f"parsl.examples.diaspora.run.{args.mode}",
        command=_run,
    )


def run_monte_carlo_workflow(args) -> int:
    if args.count < 1:
        raise ValueError("--count must be >= 1")

    def _run(context) -> None:
        logger = context.logger
        is_aurora = context.args.mode == AURORA_MODE
        workflow_start = time.time()

        _log_common_workflow_start(context, logger)
        logger.info(
            "Launching Monte Carlo Pi with %d workers and %d points per worker.",
            context.args.count,
            MONTE_CARLO_POINTS_PER_WORKER,
        )

        config = make_config_for_mode(context.args.mode, context.args.count, logger)
        with loaded_parsl(config) as dfk:
            logger.info("Parsl loaded. run_id=%s", getattr(dfk, "run_id", "<unknown>"))

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
                logger.info(start_msg)
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
                logger.info(finish_msg)

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
                logger.info(
                    "Submitted worker %d (task_id=%s)",
                    i,
                    getattr(future, "tid", "n/a"),
                )
            aggregate_future = aggregate_pi(*worker_futures)
            logger.info(
                "Submitted final aggregation worker (task_id=%s) waiting on %d workers",
                getattr(aggregate_future, "tid", "n/a"),
                len(worker_futures),
            )

            pending = {f: i for i, f in enumerate(worker_futures)}
            worker_results: dict[int, dict] = {}
            heartbeat_seconds = (
                AURORA_MONITORING_INTERVAL_SECONDS
                if is_aurora
                else LOCAL_MONITORING_INTERVAL_SECONDS
            )
            logger.info("Heartbeat interval: %.1fs", heartbeat_seconds)
            while pending:
                completed_now = [f for f in pending if f.done()]
                for future in completed_now:
                    worker_id = pending.pop(future)
                    result = future.result()
                    worker_results[worker_id] = result
                    if is_aurora:
                        # Re-emit worker logs on submit side through the Diaspora logger.
                        for worker_log in result.get("worker_logs", []):
                            logger.info("%s", worker_log)
                    logger.info(
                        "Worker %d complete: inside=%d/%d partial_pi=%.8f duration=%.2fs",
                        worker_id,
                        result["inside"],
                        result["samples"],
                        result["pi_partial"],
                        result["duration_s"],
                    )

                elapsed = time.time() - monte_carlo_start
                pending_ids = sorted(pending.values())
                logger.info(
                    "Monte Carlo heartbeat: elapsed=%.1fs completed=%d/%d pending=%d pending_ids=%s final_worker_done=%s",
                    elapsed,
                    context.args.count - len(pending),
                    context.args.count,
                    len(pending),
                    pending_ids,
                    aggregate_future.done(),
                )
                if pending:
                    time.sleep(heartbeat_seconds)

            logger.info("All worker estimates finished; waiting for final aggregation worker.")
            summary = aggregate_future.result()
            total_monte_carlo_s = time.time() - monte_carlo_start
            logger.info(
                "Final Pi estimate=%.10f absolute_error=%.10f using workers=%d total_samples=%d total_inside=%d",
                summary["pi_estimate"],
                summary["absolute_error"],
                summary["worker_count"],
                summary["total_samples"],
                summary["total_inside"],
            )
            logger.info(
                "Best partial worker by absolute error: worker_id=%d",
                summary["best_partial_worker"],
            )
            logger.info(
                "Completed Monte Carlo Pi workflow with %d workers in %.1fs. Log records were sent to Diaspora topic '%s'.",
                context.args.count,
                total_monte_carlo_s,
                context.topic,
            )

        logger.info("Workflow wall-clock time: %.1fs", time.time() - workflow_start)

    return execute_logged_command(
        args,
        logger_name=f"parsl.examples.diaspora.monte_carlo.{args.mode}",
        command=_run,
    )
