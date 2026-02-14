#!/usr/bin/env python3
"""Minimal Parsl script with Diaspora-backed debug logging."""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Callable

import parsl
from parsl.addresses import address_by_hostname
from parsl.app.app import python_app
from parsl.config import Config
from parsl.executors import HighThroughputExecutor, ThreadPoolExecutor
from parsl.launchers import MpiExecLauncher
from parsl.monitoring import MonitoringHub
from parsl.providers import PBSProProvider
from parsl.retries.types import RetryDecision

DIASPORA_FORMAT = "DIASPORA|%(levelname)s|%(name)s|%(funcName)s:%(lineno)d|%(message)s"
DEFAULT_RETRY_BUDGET = 1
LOCAL_MODE = "local"
AURORA_MODE = "aurora"
AURORA_ACCOUNT = "Diaspora"
LOCAL_TOPIC = "topic-parsl-local"
AURORA_TOPIC = "topic-parsl-aurora-debug"
LOCAL_MONITORING_INTERVAL_SECONDS = 0.5
AURORA_MONITORING_INTERVAL_SECONDS = 10
SCRIPT_DIR = Path(__file__).resolve().parent
RETRY_POLICY_ONCE = "once"
RETRY_POLICY_LLM_MINIMAX = "llm-minimax"
DEFAULT_MINIMAX_MODEL = "MiniMax-M2.5"

retry_policy_logger = logging.getLogger("parsl.examples.minimal_tstring_probe.retry_policy")
retry_llm_logger = logging.getLogger("parsl.examples.minimal_tstring_probe.retry_llm_policy")


def retry_once_policy(_exception: Exception, task_record: dict[str, object]) -> float:
    retry_policy_logger.warning("retry_once_policy task_record=%r", task_record)
    fail_count = int(task_record.get("fail_count", 0))
    if fail_count <= DEFAULT_RETRY_BUDGET:
        return 1.0
    return float(DEFAULT_RETRY_BUDGET + 1)


def build_retry_once_policy(retry_budget: int) -> Callable[[Exception, dict[str, object]], float]:
    def retry_once_policy_for_budget(_exception: Exception, task_record: dict[str, object]) -> float:
        retry_policy_logger.warning("retry_once_policy task_record=%r", task_record)
        fail_count = int(task_record.get("fail_count", 0))
        if fail_count <= retry_budget:
            return 1.0
        return float(retry_budget + 1)

    return retry_once_policy_for_budget


def build_retry_handler(
    *,
    retry_policy: str,
    topic: str,
    minimax_model: str,
    retry_budget: int,
) -> Callable[[Exception, dict[str, object]], RetryDecision]:
    if retry_policy == RETRY_POLICY_ONCE:
        return build_retry_once_policy(retry_budget)

    if retry_policy == RETRY_POLICY_LLM_MINIMAX:
        client = parsl.MiniMaxOpenAICompatClient()
        retry_llm_logger.info(
            "Using LLM retry policy with MiniMax model=%s topic=%s",
            minimax_model,
            topic,
        )
        return parsl.build_retry_llm_policy(
            llm_client=client,
            diaspora_topic=topic,
            model=minimax_model,
            policy_logger=retry_llm_logger,
        )

    raise ValueError(f"Unsupported retry policy: {retry_policy}")


def default_topic_for_mode(mode: str) -> str:
    if mode == LOCAL_MODE:
        return LOCAL_TOPIC
    if mode == AURORA_MODE:
        return AURORA_TOPIC
    raise ValueError(f"Unsupported mode: {mode}")


def make_monitoring_config(mode: str) -> MonitoringHub:
    if mode == LOCAL_MODE:
        interval = LOCAL_MONITORING_INTERVAL_SECONDS
    elif mode == AURORA_MODE:
        interval = AURORA_MONITORING_INTERVAL_SECONDS
    else:
        raise ValueError(f"Unsupported mode: {mode}")

    return MonitoringHub(
        monitoring_debug=True,
        resource_monitoring_enabled=True,
        resource_monitoring_interval=interval,
    )


def make_local_config(
    retry_handler: Callable[[Exception, dict[str, object]], RetryDecision],
    retries: int,
) -> Config:
    return Config(
        executors=[ThreadPoolExecutor(label="local_threads", max_threads=1)],
        monitoring=make_monitoring_config(mode=LOCAL_MODE),
        retries=retries,
        retry_handler=retry_handler,
        initialize_logging=False,
    )


def make_aurora_config(
    retry_handler: Callable[[Exception, dict[str, object]], RetryDecision],
    retries: int,
) -> tuple[Config, str, str]:
    account = AURORA_ACCOUNT
    queue = "debug"

    venv = os.environ.get("VIRTUAL_ENV")
    worker_init_parts = [
        "export TMPDIR=/tmp",
        "export TEMP=/tmp",
        "export TMP=/tmp",
        f"export PYTHONPATH={SCRIPT_DIR}:${{PYTHONPATH:-}}",
    ]
    if venv:
        worker_init_parts.append(f"source {venv}/bin/activate")
    worker_init = "; ".join(worker_init_parts)

    config = Config(
        executors=[
            HighThroughputExecutor(
                label="aurora_htex",
                address=address_by_hostname(),
                worker_debug=True,
                provider=PBSProProvider(
                    account=account,
                    queue=queue,
                    walltime="00:15:00",
                    worker_init=worker_init,
                    scheduler_options="#PBS -l filesystems=home:flare",
                    launcher=MpiExecLauncher(bind_cmd="--cpu-bind", overrides="--depth=64 --ppn 1"),
                    select_options="",
                    nodes_per_block=1,
                    cpus_per_node=64,
                    init_blocks=1,
                    min_blocks=1,
                    max_blocks=1,
                ),
            )
        ],
        monitoring=make_monitoring_config(mode=AURORA_MODE),
        retries=retries,
        retry_handler=retry_handler,
        initialize_logging=False,
    )
    return config, account, queue


def make_config_for_mode(
    mode: str,
    logger: logging.Logger,
    retry_handler: Callable[[Exception, dict[str, object]], RetryDecision],
    retries: int,
) -> Config:
    account = "n/a"
    queue = "n/a"
    if mode == AURORA_MODE:
        config, account, queue = make_aurora_config(retry_handler=retry_handler, retries=retries)
    elif mode == LOCAL_MODE:
        config = make_local_config(retry_handler=retry_handler, retries=retries)
    else:
        raise ValueError(f"Unsupported mode: {mode}")

    logger.info(
        "Execution profile selected: mode=%s account=%s queue=%s",
        mode,
        account,
        queue,
    )
    return config


@python_app
def pep750_tstring_probe() -> object:
    namespace = {"name": "parsl"}
    exec('template = t"Hello {name}"', namespace, namespace)
    return namespace["template"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Parsl with Diaspora-backed debug logging.")
    parser.add_argument(
        "--mode",
        choices=[LOCAL_MODE, AURORA_MODE],
        default=LOCAL_MODE,
        help="Execution mode: local thread pool or Aurora PBS.",
    )
    parser.add_argument("--topic", default=None, help="Diaspora topic name.")
    parser.add_argument(
        "--retry-policy",
        choices=[RETRY_POLICY_ONCE, RETRY_POLICY_LLM_MINIMAX],
        default=RETRY_POLICY_ONCE,
        help="Retry behavior: classic one-time retry or LLM runtime patching.",
    )
    parser.add_argument(
        "--minimax-model",
        default=DEFAULT_MINIMAX_MODEL,
        help="MiniMax model name for --retry-policy llm-minimax.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=DEFAULT_RETRY_BUDGET,
        help="Retry budget/count for Parsl task retries.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.retries < 0:
        raise ValueError("--retries must be >= 0")
    topic = args.topic or default_topic_for_mode(args.mode)

    close_stream_logger = parsl.set_stream_logger(name="parsl", level=logging.DEBUG)
    close_diaspora_logger = parsl.set_diaspora_logger(
        topic_name=topic,
        name="parsl",
        level=logging.DEBUG,
        format_string=DIASPORA_FORMAT,
    )
    logger = logging.getLogger("parsl.examples.minimal_tstring_probe")

    loaded = False
    try:
        logger.info("If this is your first run, execute: python examples/diaspora/diaspora.py setup")
        logger.info("Running with mode=%s and verbose debug logging.", args.mode)
        logger.info("Resolved logging target topic=%s", topic)
        logger.info("Retry policy selected: %s", args.retry_policy)
        logger.info("Retry count selected: %s", args.retries)

        retry_handler = build_retry_handler(
            retry_policy=args.retry_policy,
            topic=topic,
            minimax_model=args.minimax_model,
            retry_budget=args.retries,
        )

        config = make_config_for_mode(
            mode=args.mode,
            logger=logger,
            retry_handler=retry_handler,
            retries=args.retries,
        )

        dfk = parsl.load(config)
        loaded = True
        logger.info("Parsl loaded. run_id=%s", getattr(dfk, "run_id", "<unknown>"))

        future = pep750_tstring_probe()
        value = future.result()
        logger.info("Task completed with value: %r", value)
    finally:
        if loaded:
            parsl.clear()

        try:
            close_diaspora_logger()
        except Exception:
            logger.exception("Failed to close Diaspora logger")
        try:
            close_stream_logger()
        except Exception:
            logger.exception("Failed to close stream logger")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
