"""Shared execution config for minimal examples."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Callable

from parsl.config import Config
from parsl.retries.types import RetryDecision

LOCAL_MODE = "local"
AURORA_MODE = "aurora"
MIDWAY_MODE = "midway"

LOCAL_TOPIC = "topic-parsl-local"
AURORA_TOPIC = "topic-parsl-aurora-debug"
MIDWAY_TOPIC = "topic-parsl-midway3-debug"

EXAMPLES_DIR = Path(__file__).resolve().parent


def default_topic_for_mode(mode: str) -> str:
    if mode == LOCAL_MODE:
        return LOCAL_TOPIC
    if mode == AURORA_MODE:
        return AURORA_TOPIC
    if mode == MIDWAY_MODE:
        return MIDWAY_TOPIC
    raise ValueError(f"Unsupported mode: {mode}")


def make_monitoring_config(mode: str):
    from parsl.monitoring import MonitoringHub

    if mode not in (LOCAL_MODE, AURORA_MODE, MIDWAY_MODE):
        raise ValueError(f"Unsupported mode: {mode}")

    return MonitoringHub(
        monitoring_debug=True,
        resource_monitoring_enabled=True,
        resource_monitoring_interval=10,
    )


def make_local_config(
    retry_handler: Callable[[Exception, dict[str, object]], RetryDecision],
    retries: int,
) -> Config:
    from parsl.executors import ThreadPoolExecutor

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
    account: str = "Diaspora",
    queue: str = "debug",
    walltime: str = "00:15:00",
) -> Config:
    from parsl.addresses import address_by_hostname
    from parsl.executors import HighThroughputExecutor
    from parsl.launchers import MpiExecLauncher
    from parsl.providers import PBSProProvider

    venv = os.environ.get("VIRTUAL_ENV")
    worker_init_parts = [
        "export TMPDIR=/tmp",
        "export TEMP=/tmp",
        "export TMP=/tmp",
        f"export PYTHONPATH={EXAMPLES_DIR}:${{PYTHONPATH:-}}",
    ]
    if venv:
        worker_init_parts.append(f"source {venv}/bin/activate")
    worker_init = "; ".join(worker_init_parts)

    config = Config(
        executors=[
            HighThroughputExecutor(
                label="aurora_htex",
                provider=PBSProProvider(
                    account=account,
                    queue=queue,
                    scheduler_options="#PBS -l filesystems=home:flare",
                    select_options="",
                    worker_init=worker_init,
                    nodes_per_block=1,
                    cpus_per_node=64,
                    init_blocks=1,
                    min_blocks=1,
                    max_blocks=1,
                    launcher=MpiExecLauncher(bind_cmd="--cpu-bind", overrides="--depth=64 --ppn 1"),
                    walltime=walltime,
                ),
                address=address_by_hostname(),
                worker_debug=True,
            )
        ],
        monitoring=make_monitoring_config(mode=AURORA_MODE),
        retries=retries,
        retry_handler=retry_handler,
        initialize_logging=False,
    )
    return config


def make_midway_config(
    retry_handler: Callable[[Exception, dict[str, object]], RetryDecision],
    retries: int,
    account: str = "pi-chard",
    partition: str = "build",
    walltime: str = "00:15:00",
) -> Config:
    from parsl.addresses import address_by_hostname
    from parsl.executors import HighThroughputExecutor
    from parsl.launchers import SrunLauncher
    from parsl.providers import SlurmProvider

    venv = os.environ.get("VIRTUAL_ENV")
    worker_init_parts = [
        "export TMPDIR=/tmp",
        "export TEMP=/tmp",
        "export TMP=/tmp",
        f"export PYTHONPATH={EXAMPLES_DIR}:${{PYTHONPATH:-}}",
        "export OMP_NUM_THREADS=1",
    ]
    if venv:
        worker_init_parts.insert(0, f"source {venv}/bin/activate")
    worker_init = "; ".join(worker_init_parts)

    config = Config(
        executors=[
            HighThroughputExecutor(
                label="midway3_htex",
                provider=SlurmProvider(
                    partition=partition,
                    account=account,
                    nodes_per_block=1,
                    init_blocks=1,
                    min_blocks=1,
                    max_blocks=1,
                    walltime=walltime,
                    worker_init=worker_init,
                    launcher=SrunLauncher(),
                ),
                address=address_by_hostname(),
                worker_debug=True,
                max_workers_per_node=1,
            )
        ],
        monitoring=make_monitoring_config(mode=MIDWAY_MODE),
        retries=retries,
        retry_handler=retry_handler,
        initialize_logging=False,
    )
    return config


def make_config_for_mode(
    mode: str,
    retry_handler: Callable[[Exception, dict[str, object]], RetryDecision],
    retries: int,
) -> Config:
    if mode == AURORA_MODE:
        return make_aurora_config(retry_handler=retry_handler, retries=retries)
    if mode == MIDWAY_MODE:
        return make_midway_config(retry_handler=retry_handler, retries=retries)
    if mode == LOCAL_MODE:
        return make_local_config(retry_handler=retry_handler, retries=retries)
    raise ValueError(f"Unsupported mode: {mode}")
