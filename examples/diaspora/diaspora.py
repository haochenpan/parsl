#!/usr/bin/env python3
"""Unified CLI for Parsl Diaspora examples.

Install dependencies from this checkout with:
    pip install -e ".[diaspora,monitoring]"
"""

from __future__ import annotations

import argparse
import logging

from config import AURORA_MODE, LOCAL_MODE
from failures import SCENARIO_CHOICES, run_failure_scenario
from runtime import parse_log_level
from topic_ops import run_clear, run_consume, run_setup
from workflows import run_hello_workflow, run_monte_carlo_workflow


def _add_mode_option(parser: argparse.ArgumentParser, help_text: str) -> None:
    parser.add_argument(
        "--mode",
        choices=[LOCAL_MODE, AURORA_MODE],
        default=LOCAL_MODE,
        help=help_text,
    )


def _add_run_options(parser: argparse.ArgumentParser, count_help: str) -> None:
    _add_mode_option(parser, "Execution mode: local thread pool or Aurora PBS.")
    parser.add_argument("--topic", default=None, help="Diaspora topic name (without namespace).")
    parser.add_argument("--count", type=int, default=3, help=count_help)
    parser.add_argument("--log-file", default=None, help="Local file logger output path.")
    parser.add_argument(
        "--log-level",
        type=parse_log_level,
        default=logging.INFO,
        help="Logging level for stream/file/Diaspora handlers (for example: DEBUG, INFO, WARNING).",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Parsl Diaspora examples CLI.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    setup_parser = subparsers.add_parser("setup", help="Run one-time Diaspora user setup.")
    setup_parser.add_argument(
        "--environment",
        default=None,
        help="Optional Diaspora SDK environment override (for example: local).",
    )
    setup_parser.set_defaults(handler=run_setup)

    run_parser = subparsers.add_parser("run", help="Run the hello-world Parsl workflow.")
    _add_run_options(run_parser, "Number of hello tasks.")
    run_parser.set_defaults(handler=run_hello_workflow)

    monte_carlo_parser = subparsers.add_parser(
        "monte-carlo",
        aliases=["ensemble"],
        help="Run Monte Carlo Pi estimation workflow.",
    )
    _add_run_options(monte_carlo_parser, "Number of Monte Carlo workers.")
    monte_carlo_parser.set_defaults(handler=run_monte_carlo_workflow)

    failure_parser = subparsers.add_parser("failure", help="Run one of the failure/retry scenarios.")
    _add_run_options(failure_parser, "Number of failing task pairs.")
    failure_parser.add_argument(
        "--scenario",
        choices=SCENARIO_CHOICES,
        required=True,
        help="Failure scenario to execute.",
    )
    failure_parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=1,
        help="Timeout used by the timeout scenario (seconds).",
    )
    failure_parser.set_defaults(handler=run_failure_scenario)

    consume_parser = subparsers.add_parser("consume", help="Consume topic events matching 'python_app'.")
    _add_mode_option(consume_parser, "Default topic profile when --topic is omitted.")
    consume_parser.add_argument("--topic", default=None, help="Diaspora topic name without namespace.")
    consume_parser.add_argument(
        "--timeout-ms",
        type=int,
        default=10000,
        help="Consumer timeout in milliseconds when reading existing records.",
    )
    consume_parser.set_defaults(handler=run_consume)

    clear_parser = subparsers.add_parser("clear", help="Recreate a topic and remove a local log file.")
    _add_mode_option(clear_parser, "Default topic/log-file profile when args are omitted.")
    clear_parser.add_argument("--topic", default=None, help="Diaspora topic name without namespace.")
    clear_parser.add_argument("--log-file", default=None, help="Path to local log file to delete.")
    clear_parser.set_defaults(handler=run_clear)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.handler(args))


if __name__ == "__main__":
    raise SystemExit(main())
