#!/usr/bin/env python3
"""Minimal Parsl hello-world script with retry policies and local/Aurora/Midway configs."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Callable
import time
import parsl
from parsl.app.app import python_app
from parsl.retries.types import RetryDecision
from dotenv import load_dotenv

from config import AURORA_MODE, LOCAL_MODE, MIDWAY_MODE, default_topic_for_mode, make_config_for_mode

RETRY_POLICY_BUDGET = "budget"
RETRY_POLICY_LLM_MINIMAX = "llm-minimax"
SCRIPT_DIR = Path(__file__).resolve().parent
DOTENV_PATH = SCRIPT_DIR / ".env"


def build_retry_handler(
    retry_policy: str,
    retry_budget: int,
    topic: str,
    time_horizon: int,
) -> Callable[[Exception, dict[str, object]], RetryDecision]:
    if retry_policy == RETRY_POLICY_BUDGET:
        def retry_budget_policy(_exception: Exception, task_record: dict[str, object]) -> RetryDecision:
            fail_count = int(task_record.get("fail_count", 0))
            if fail_count <= retry_budget:
                return 1.0
            return float(retry_budget + 1)
        return retry_budget_policy
    
    if retry_policy == RETRY_POLICY_LLM_MINIMAX:
        return parsl.build_retry_llm_policy(
            llm_client= parsl.MiniMaxOpenAICompatClient(),
            diaspora_topic=topic,
            diaspora_time_horizon=time_horizon,
        )
    raise ValueError(f"Unsupported retry policy: {retry_policy}")


@python_app
def pep750_tstring_probe() -> object:
    namespace = {"name": "parsl"}
    exec('template = t"Hello {name}"', namespace, namespace)
    return namespace["template"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a minimal Parsl t-string hello-world app.")
    parser.add_argument(
        "--config",
        choices=[LOCAL_MODE, AURORA_MODE, MIDWAY_MODE],
        default=LOCAL_MODE,
        help="Execution config: local thread pool, Aurora PBS, or Midway3 Slurm.",
    )
    parser.add_argument(
        "--retry-policy",
        choices=[RETRY_POLICY_BUDGET, RETRY_POLICY_LLM_MINIMAX],
        default=RETRY_POLICY_BUDGET,
        help="Retry behavior: budgeted retry or LLM runtime patching.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=1,
        help="Retry budget/count for Parsl task retries.",
    )
    parser.add_argument("--topic", default=None, help="Diaspora topic name for LLM retry context.")

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.retries < 0:
        raise ValueError("--retries must be >= 0")

    topic = args.topic or default_topic_for_mode(args.config)
    logger = logging.getLogger("parsl.examples.minimal_tstring_probe")
    run_extra: dict[str, object] = {}

    diaspora_time_horizon = int(time.time() * 1000)
    close_stream_logger = parsl.set_stream_logger(name="parsl", level=logging.DEBUG)
    close_diaspora_logger = parsl.set_diaspora_logger(
        topic_name=topic,
        name="parsl",
        level=logging.DEBUG,
    )

    loaded = False
    try:
        logger.info(
            "Launching minimal t-string probe with config=%s retry_policy=%s retries=%d topic=%s",
            args.config,
            args.retry_policy,
            args.retries,
            topic,
            extra=run_extra,
        )

        if args.retry_policy == RETRY_POLICY_LLM_MINIMAX:
            load_dotenv(dotenv_path=DOTENV_PATH, override=False)

        retry_handler = build_retry_handler(
            retry_policy=args.retry_policy,
            retry_budget=args.retries,
            topic=topic,
            time_horizon=diaspora_time_horizon,
        )
        config = make_config_for_mode(
            mode=args.config,
            retry_handler=retry_handler,
            retries=args.retries,
        )

        dfk = parsl.load(config)
        loaded = True
        run_id = getattr(dfk, "run_id", None)
        run_extra = {"run_id": run_id} if run_id is not None else {}
        logger.info(
            "Parsl loaded. run_id=%s",
            run_id if run_id is not None else "<unknown>",
            extra=run_extra,
        )

        future = pep750_tstring_probe()
        task_id = getattr(future, "tid", None)
        task_extra = dict(run_extra)
        if task_id is not None:
            task_extra["task_id"] = task_id
        logger.info(
            "Submitted pep750_tstring_probe task (task_id=%s)",
            task_id if task_id is not None else "n/a",
            extra=task_extra,
        )

        try:
            value = future.result()
            logger.info(
                "Task completed with value: %r",
                value,
                extra=task_extra,
            )
            print(value)
        except Exception:
            logger.exception("Task failed", extra=task_extra)
            raise
    finally:
        if loaded:
            parsl.clear()
            logger.info("Parsl cleared", extra=run_extra)

        try:
            close_diaspora_logger()
        except Exception:
            pass

        try:
            close_stream_logger()
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
