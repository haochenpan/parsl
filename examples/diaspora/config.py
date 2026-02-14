"""Shared config primitives for diaspora examples."""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from parsl.config import Config

LOCAL_MODE = "local"
AURORA_MODE = "aurora"
DIASPORA_DEMO_FORMAT = "DIASPORA|%(levelname)s|%(name)s|%(funcName)s:%(lineno)d|%(message)s"

LOCAL_TOPIC = "topic-parsl-local"
LOCAL_LOG_FILE = "parsl-local.log"
AURORA_TOPIC = "topic-parsl-aurora-debug"
AURORA_LOG_FILE = "parsl-aurora-debug.log"

AURORA_ACCOUNT = "Diaspora"
LOCAL_MONITORING_INTERVAL_SECONDS = 0.5
AURORA_MONITORING_INTERVAL_SECONDS = 10
DIASPORA_MODULE_DIR = Path(__file__).resolve().parent


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


def make_monitoring_config(mode: str):
    from parsl.monitoring import MonitoringHub

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


def make_local_config() -> Config:
    from parsl.config import Config
    from parsl.executors import ThreadPoolExecutor

    return Config(
        executors=[ThreadPoolExecutor(label="local_threads", max_threads=2)],
        monitoring=make_monitoring_config(mode=LOCAL_MODE),
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
        f"export PYTHONPATH={DIASPORA_MODULE_DIR}:${{PYTHONPATH:-}}",
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
                    nodes_per_block=2 if queue == "debug-scaling" else 1,
                    cpus_per_node=64,
                    init_blocks=1,
                    min_blocks=1,
                    max_blocks=1,
                ),
            )
        ],
        monitoring=make_monitoring_config(mode=AURORA_MODE),
        initialize_logging=False,
    )
    return config, account, queue
