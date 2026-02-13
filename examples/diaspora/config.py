"""Shared config for diaspora example runners/utilities."""

from __future__ import annotations

import argparse
import logging
import os

LOCAL_MODE = "local"
AURORA_MODE = "aurora"
DIASPORA_DEMO_FORMAT = "DIASPORA|%(levelname)s|%(name)s|%(funcName)s:%(lineno)d|%(message)s"

LOCAL_TOPIC = "topic-parsl-local"
LOCAL_LOG_FILE = "parsl-local.log"
AURORA_TOPIC = "topic-parsl-aurora-debug"
AURORA_LOG_FILE = "parsl-aurora-debug.log"

AURORA_ACCOUNT = "Diaspora"


def resolve_aurora_queue(count: int) -> str:
    return "debug" if count in (1, 2) else "debug-scaling"


def resolve_aurora_profile(count: int) -> tuple[str, str]:
    return AURORA_ACCOUNT, resolve_aurora_queue(count)


def defaults_for_mode(mode: str) -> tuple[str, str]:
    if mode == LOCAL_MODE:
        return LOCAL_TOPIC, LOCAL_LOG_FILE
    if mode == AURORA_MODE:
        return AURORA_TOPIC, AURORA_LOG_FILE
    raise ValueError(f"Unsupported mode: {mode}")


def parse_run_args(
    description: str,
    count_help: str,
    default_mode: str = LOCAL_MODE,
) -> argparse.Namespace:
    # Local import avoids module cycle: util imports defaults_for_mode from this module.
    from util import parse_log_level

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--mode",
        choices=[LOCAL_MODE, AURORA_MODE],
        default=default_mode,
        help="Execution mode: local thread pool or Aurora PBS.",
    )
    parser.add_argument("--topic", default=None, help="Diaspora topic name (without namespace).")
    parser.add_argument("--count", type=int, default=3, help=count_help)
    parser.add_argument("--log-file", default=None, help="Local file logger output path.")
    parser.add_argument(
        "--log-level",
        type=parse_log_level,
        default=logging.INFO,
        help="Logging level for stream/file/Diaspora handlers (for example: DEBUG, INFO, WARNING).",
    )
    return parser.parse_args()


def make_local_config() -> Config:
    from parsl.config import Config
    from parsl.executors import ThreadPoolExecutor

    return Config(
        executors=[ThreadPoolExecutor(label="local_threads", max_threads=2)],
        initialize_logging=False,
    )


def make_aurora_config(count: int) -> tuple[Config, str, str]:
    from parsl.addresses import address_by_hostname
    from parsl.config import Config
    from parsl.executors import HighThroughputExecutor
    from parsl.launchers import MpiExecLauncher
    from parsl.providers import PBSProProvider

    account, queue = resolve_aurora_profile(count)
    venv = os.environ.get("VIRTUAL_ENV")
    worker_init_parts = [
        "export TMPDIR=/tmp",
        "export TEMP=/tmp",
        "export TMP=/tmp",
    ]
    if venv:
        worker_init_parts.append(f"source {venv}/bin/activate")
    worker_init = "; ".join(worker_init_parts)

    config = Config(
        executors=[
            HighThroughputExecutor(
                label="aurora_htex",
                address=address_by_hostname(),
                provider=PBSProProvider(
                    account=account,
                    queue=queue,
                    walltime="00:05:00",
                    worker_init=worker_init,
                    scheduler_options="#PBS -l filesystems=home:flare",
                    launcher=MpiExecLauncher(bind_cmd="--cpu-bind", overrides="--depth=64 --ppn 1"),
                    select_options="",
                    nodes_per_block=2 if queue == "debug-scaling" else 1,
                    cpus_per_node=64,
                    init_blocks=1,
                    min_blocks=1,
                    max_blocks=1,
                ),
            )
        ],
        initialize_logging=False,
    )
    return config, account, queue
