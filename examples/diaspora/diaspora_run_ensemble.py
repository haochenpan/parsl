#!/usr/bin/env python3
"""Parsl ensemble workflow with post-processing analysis and Diaspora logging.

Install dependencies from this checkout with:
    pip install -e ".[diaspora,monitoring]"

Run one-time user setup first:
    python examples/diaspora/diaspora_setup.py
"""

from __future__ import annotations

import json
import logging
import time

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

# Keep runtime noticeably longer than the hello-world script and emit richer logs.
ENSEMBLE_STEPS = 30
STEP_SLEEP_SECONDS = 0.25
LOCAL_HEARTBEAT_SECONDS = 0.5
AURORA_HEARTBEAT_SECONDS = 10.0


def main(default_mode: str = "local") -> None:
    args = parse_run_args(
        description="Run an ensemble + analysis Parsl app and send log events to Diaspora.",
        count_help="Number of ensemble members.",
        default_mode=default_mode,
    )
    is_aurora = args.mode == AURORA_MODE
    if args.count < 1:
        raise ValueError("--count must be >= 1")

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
    logger = logging.getLogger(f"parsl.examples.diaspora.ensemble.{args.mode}")
    loaded = False
    workflow_start = time.time()

    try:
        try:
            logger.info("If this is your first run, execute: python examples/diaspora/diaspora_setup.py")
            logger.info("Kafka events include both raw 'message' and formatter-rendered 'formatted' fields.")
            logger.info("Execution mode: %s", args.mode)
            logger.info(
                "Resolved logging targets: topic=%s log_file=%s level=%s",
                topic,
                log_file,
                logging.getLevelName(args.log_level),
            )
            logger.info(
                "Launching %d ensemble members with %d steps each (step sleep %.2fs).",
                args.count,
                ENSEMBLE_STEPS,
                STEP_SLEEP_SECONDS,
            )
            est_member_runtime = ENSEMBLE_STEPS * STEP_SLEEP_SECONDS
            logger.info(
                "Rough runtime estimate: %.1fs/member before overhead (not counting queue/warmup).",
                est_member_runtime,
            )

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
            dfk = parsl.load(config)
            loaded = True
            logger.info("Parsl loaded. run_id=%s", getattr(dfk, "run_id", "<unknown>"))

            @python_app
            def simulate_member(
                member_id: int,
                steps: int,
                step_sleep_s: float,
            ) -> dict:
                import random
                import statistics
                import time as _time

                worker_logs = []
                start_msg = f"[python_app] member {member_id} starting with steps={steps} step_sleep_s={step_sleep_s:.2f}"
                worker_logs.append(start_msg)
                logger.info(start_msg)
                rng = random.Random(4100 + member_id)
                values = []
                start = _time.time()
                current = 50.0 + member_id
                for step in range(steps):
                    current += rng.gauss(0.0, 1.2)
                    values.append(current)
                    if step in {0, steps // 2, steps - 1}:
                        progress_msg = f"[python_app] member {member_id} progress step={step} value={current:.3f}"
                        worker_logs.append(progress_msg)
                        logger.info(progress_msg)
                    _time.sleep(step_sleep_s + rng.random() * 0.05)
                end = _time.time()
                finish_msg = f"[python_app] member {member_id} finished in {end - start:.2f}s"
                worker_logs.append(finish_msg)
                logger.info(finish_msg)

                return {
                    "member_id": member_id,
                    "steps": steps,
                    "duration_s": end - start,
                    "mean": statistics.fmean(values),
                    "stdev": statistics.pstdev(values) if len(values) > 1 else 0.0,
                    "min": min(values),
                    "max": max(values),
                    "last3": values[-3:],
                    "worker_logs": worker_logs,
                }

            @python_app
            def analyze_ensemble(*member_summaries: dict) -> dict:
                import statistics

                summaries = list(member_summaries)
                ordered = sorted(summaries, key=lambda x: x["mean"], reverse=True)
                means = [x["mean"] for x in summaries]
                durations = [x["duration_s"] for x in summaries]
                return {
                    "ensemble_size": len(summaries),
                    "mean_of_means": statistics.fmean(means),
                    "mean_stdev": statistics.pstdev(means) if len(means) > 1 else 0.0,
                    "spread": max(means) - min(means),
                    "fastest_member": min(summaries, key=lambda x: x["duration_s"])["member_id"],
                    "slowest_member": max(summaries, key=lambda x: x["duration_s"])["member_id"],
                    "avg_member_duration_s": statistics.fmean(durations),
                    "top_members_by_mean": [x["member_id"] for x in ordered[:3]],
                }

            ensemble_start = time.time()
            member_futures = []
            for i in range(args.count):
                future = simulate_member(i, ENSEMBLE_STEPS, STEP_SLEEP_SECONDS)
                member_futures.append(future)
                logger.info(
                    "Submitted member %d (task_id=%s)",
                    i,
                    getattr(future, "tid", "n/a"),
                )
            analysis_future = analyze_ensemble(*member_futures)
            logger.info(
                "Submitted analysis task (task_id=%s) waiting on %d member futures",
                getattr(analysis_future, "tid", "n/a"),
                len(member_futures),
            )

            pending = {f: i for i, f in enumerate(member_futures)}
            member_results: dict[int, dict] = {}
            heartbeat_seconds = AURORA_HEARTBEAT_SECONDS if is_aurora else LOCAL_HEARTBEAT_SECONDS
            logger.info("Heartbeat interval: %.1fs", heartbeat_seconds)
            while pending:
                completed_now = [f for f in pending if f.done()]
                for future in completed_now:
                    member_id = pending.pop(future)
                    result = future.result()
                    member_results[member_id] = result
                    if is_aurora:
                        # Worker logs are collected in result payload and re-emitted
                        # from submit side so they go through Diaspora logger.
                        for worker_log in result.get("worker_logs", []):
                            logger.info("%s", worker_log)
                    logger.info(
                        "Member %d complete: mean=%.3f stdev=%.3f range=[%.3f, %.3f] duration=%.2fs last3=%s",
                        member_id,
                        result["mean"],
                        result["stdev"],
                        result["min"],
                        result["max"],
                        result["duration_s"],
                        [round(v, 3) for v in result["last3"]],
                    )

                if completed_now and member_results:
                    completed_count = len(member_results)
                    sample_slot = completed_count % args.count
                    completed_ids = sorted(member_results)
                    sampled_member_id = completed_ids[sample_slot % len(completed_ids)]
                    sampled_member = member_results[sampled_member_id]
                    logger.info(
                        "Sampled member payload for Diaspora (completed=%d/%d sample_slot=%d sampled_member_id=%d): %s",
                        completed_count,
                        args.count,
                        sample_slot,
                        sampled_member_id,
                        json.dumps(
                            {
                                "member_id": sampled_member["member_id"],
                                "steps": sampled_member["steps"],
                                "duration_s": round(sampled_member["duration_s"], 3),
                                "mean": round(sampled_member["mean"], 6),
                                "stdev": round(sampled_member["stdev"], 6),
                                "min": round(sampled_member["min"], 6),
                                "max": round(sampled_member["max"], 6),
                                "last3": [round(v, 6) for v in sampled_member["last3"]],
                            },
                            sort_keys=True,
                        ),
                    )

                elapsed = time.time() - ensemble_start
                pending_ids = sorted(pending.values())
                logger.info(
                    "Ensemble heartbeat: elapsed=%.1fs completed=%d/%d pending=%d pending_ids=%s analysis_done=%s",
                    elapsed,
                    args.count - len(pending),
                    args.count,
                    len(pending),
                    pending_ids,
                    analysis_future.done(),
                )
                if pending:
                    time.sleep(heartbeat_seconds)

            logger.info("All ensemble members finished; waiting for analysis app to finalize.")
            summary = analysis_future.result()
            total_ensemble_s = time.time() - ensemble_start
            logger.info(
                "Analysis complete: ensemble_size=%d mean_of_means=%.3f spread=%.3f mean_stdev=%.3f",
                summary["ensemble_size"],
                summary["mean_of_means"],
                summary["spread"],
                summary["mean_stdev"],
            )
            logger.info(
                "Analysis details: fastest_member=%d slowest_member=%d avg_member_duration_s=%.2f top_members_by_mean=%s",
                summary["fastest_member"],
                summary["slowest_member"],
                summary["avg_member_duration_s"],
                summary["top_members_by_mean"],
            )
            logger.info(
                "Completed ensemble workflow with %d members in %.1fs. Log records were sent to Diaspora topic '%s'.",
                args.count,
                total_ensemble_s,
                topic,
            )
        finally:
            if loaded:
                logger.info("Clearing Parsl DataFlowKernel.")
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
        logger.info("Workflow wall-clock time: %.1fs", time.time() - workflow_start)
    finally:
        close_stream_logger()


if __name__ == "__main__":
    main()
