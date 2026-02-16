#!/usr/bin/env python3
"""Minimal Parsl hello-world script with retry policies and local/Aurora modes."""

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

LOCAL_MODE = "local"
AURORA_MODE = "aurora"
LOCAL_TOPIC = "topic-parsl-local"
AURORA_TOPIC = "topic-parsl-aurora-debug"
AURORA_ACCOUNT = "Diaspora"
LOCAL_MONITORING_INTERVAL_SECONDS = 0.5
AURORA_MONITORING_INTERVAL_SECONDS = 10
DEFAULT_RETRY_BUDGET = 1
RETRY_POLICY_ONCE = "once"
RETRY_POLICY_LLM_MINIMAX = "llm-minimax"
DEFAULT_MINIMAX_MODEL = "MiniMax-M2.5"
SCRIPT_DIR = Path(__file__).resolve().parent
DOTENV_PATH = SCRIPT_DIR / ".env"


def default_topic_for_mode(mode: str) -> str:
    if mode == LOCAL_MODE:
        return LOCAL_TOPIC
    if mode == AURORA_MODE:
        return AURORA_TOPIC
    raise ValueError(f"Unsupported mode: {mode}")


def build_retry_once_policy(retry_budget: int) -> Callable[[Exception, dict[str, object]], RetryDecision]:
    def retry_once_policy(_exception: Exception, task_record: dict[str, object]) -> RetryDecision:
        fail_count = int(task_record.get("fail_count", 0))
        if fail_count <= retry_budget:
            return 1.0
        return float(retry_budget + 1)

    return retry_once_policy


def build_retry_llm_policy(topic: str, minimax_model: str) -> Callable[[Exception, dict[str, object]], RetryDecision]:
    client = parsl.MiniMaxOpenAICompatClient()
    return parsl.build_retry_llm_policy(
        llm_client=client,
        diaspora_topic=topic,
        model=minimax_model,
    )


def load_minimax_api_key_from_dotenv(
    *,
    logger: logging.Logger,
    log_extra: dict[str, object],
    dotenv_path: Path = DOTENV_PATH,
) -> None:
    try:
        from dotenv import load_dotenv
    except ImportError as exc:
        raise RuntimeError(
            "python-dotenv is required for --retry-policy llm-minimax. "
            "Install with: pip install python-dotenv"
        ) from exc

    if not dotenv_path.exists():
        logger.warning(
            "dotenv file not found at %s; relying on existing environment variables.",
            dotenv_path,
            extra=log_extra,
        )
        return

    load_dotenv(dotenv_path=dotenv_path, override=False)
    logger.info(
        "Loaded dotenv file from %s (MINIMAX_API_KEY present=%s)",
        dotenv_path,
        bool(os.environ.get("MINIMAX_API_KEY")),
        extra=log_extra,
    )


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
        return build_retry_llm_policy(topic=topic, minimax_model=minimax_model)
    raise ValueError(f"Unsupported retry policy: {retry_policy}")


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
    retry_handler: Callable[[Exception, dict[str, object]], RetryDecision],
    retries: int,
) -> Config:
    if mode == AURORA_MODE:
        config, _, _ = make_aurora_config(retry_handler=retry_handler, retries=retries)
        return config
    if mode == LOCAL_MODE:
        return make_local_config(retry_handler=retry_handler, retries=retries)
    raise ValueError(f"Unsupported mode: {mode}")


@python_app
def pep750_tstring_probe() -> object:
    namespace = {"name": "parsl"}
    exec('template = t"Hello {name}"', namespace, namespace)
    return namespace["template"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a minimal Parsl t-string hello-world app.")
    parser.add_argument(
        "--mode",
        choices=[LOCAL_MODE, AURORA_MODE],
        default=LOCAL_MODE,
        help="Execution mode: local thread pool or Aurora PBS.",
    )
    parser.add_argument("--topic", default=None, help="Diaspora topic name for LLM retry context.")
    parser.add_argument(
        "--retry-policy",
        choices=[RETRY_POLICY_ONCE, RETRY_POLICY_LLM_MINIMAX],
        default=RETRY_POLICY_ONCE,
        help="Retry behavior: one-time retry or LLM runtime patching.",
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
    logger = logging.getLogger("parsl.examples.minimal_tstring_probe")
    run_extra: dict[str, object] = {}

    close_stream_logger = parsl.set_stream_logger(name="parsl", level=logging.DEBUG)
    close_diaspora_logger = parsl.set_diaspora_logger(
        topic_name=topic,
        name="parsl",
        level=logging.DEBUG,
    )

    loaded = False
    try:
        logger.info(
            "Launching minimal t-string probe with mode=%s retry_policy=%s retries=%d topic=%s",
            args.mode,
            args.retry_policy,
            args.retries,
            topic,
            extra=run_extra,
        )

        if args.retry_policy == RETRY_POLICY_LLM_MINIMAX:
            load_minimax_api_key_from_dotenv(logger=logger, log_extra=run_extra)

        retry_handler = build_retry_handler(
            retry_policy=args.retry_policy,
            topic=topic,
            minimax_model=args.minimax_model,
            retry_budget=args.retries,
        )
        config = make_config_for_mode(
            mode=args.mode,
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
