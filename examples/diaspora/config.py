"""Shared config for diaspora example runners/utilities."""

from __future__ import annotations

import argparse
import logging
import os
import sys
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
MONITORING_INTERVAL_SECONDS = 10


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


def make_monitoring_config():
    from parsl.monitoring import MonitoringHub

    return MonitoringHub(
        monitoring_debug=True,
        resource_monitoring_enabled=True,
        resource_monitoring_interval=MONITORING_INTERVAL_SECONDS,
    )


def make_local_config() -> Config:
    from parsl.config import Config
    from parsl.executors import ThreadPoolExecutor

    return Config(
        executors=[ThreadPoolExecutor(label="local_threads", max_threads=2)],
        monitoring=make_monitoring_config(),
        initialize_logging=False,
    )


def make_aurora_config(count: int) -> tuple[Config, str, str]:
    from parsl.addresses import address_by_hostname
    from parsl.config import Config
    from parsl.executors import HighThroughputExecutor
    from parsl.launchers import MpiExecLauncher
    from parsl.providers import PBSProProvider

    account, queue = resolve_aurora_profile(count)
    python_bin = Path(sys.executable).parent
    env_venv = os.environ.get("VIRTUAL_ENV")
    candidate_bins = [python_bin]
    if env_venv:
        candidate_bins.insert(0, Path(env_venv) / "bin")

    htex_script_bin = python_bin
    for bin_dir in candidate_bins:
        if (bin_dir / "interchange.py").exists() and (bin_dir / "process_worker_pool.py").exists():
            htex_script_bin = bin_dir
            break

    venv = env_venv or str(htex_script_bin.parent)
    worker_init_parts = [
        "export TMPDIR=/tmp",
        "export TEMP=/tmp",
        "export TMP=/tmp",
    ]
    if venv:
        worker_init_parts.append(f"source {venv}/bin/activate")
    worker_init = "; ".join(worker_init_parts)
    launch_cmd = (
        f"{htex_script_bin}/process_worker_pool.py "
        "{debug} {max_workers_per_node} "
        "-a {addresses} "
        "-p {prefetch_capacity} "
        "-c {cores_per_worker} "
        "-m {mem_per_worker} "
        "--poll {poll_period} "
        "--port={worker_port} "
        "--cert_dir {cert_dir} "
        "--logdir={logdir} "
        "--block_id={{block_id}} "
        "--hb_period={heartbeat_period} "
        "{address_probe_timeout_string} "
        "--hb_threshold={heartbeat_threshold} "
        "--drain_period={drain_period} "
        "--cpu-affinity {cpu_affinity} "
        "{enable_mpi_mode} "
        "--mpi-launcher={mpi_launcher} "
        "--available-accelerators {accelerators}"
    )
    interchange_launch_cmd = [f"{htex_script_bin}/interchange.py"]

    config = Config(
        executors=[
            HighThroughputExecutor(
                label="aurora_htex",
                address=address_by_hostname(),
                launch_cmd=launch_cmd,
                interchange_launch_cmd=interchange_launch_cmd,
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
        monitoring=make_monitoring_config(),
        initialize_logging=False,
    )
    return config, account, queue
