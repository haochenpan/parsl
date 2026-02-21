#!/usr/bin/env python3
"""Unified CLI for Parsl Diaspora examples.

Install dependencies from this checkout with:
    pip install -e ".[diaspora,monitoring]"
"""

from __future__ import annotations

import argparse

from topic_ops import run_clear, run_context, run_setup


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Parsl Diaspora examples CLI.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    setup_parser = subparsers.add_parser("setup", help="Run one-time Diaspora user setup.")
    setup_parser.set_defaults(handler=run_setup)

    context_parser = subparsers.add_parser("context", help="Fetch context events from a given timestamp.")
    context_parser.add_argument("--topic", default="topic-parsl-local", help="Diaspora topic name without namespace.")
    context_parser.add_argument("--time-horizon", type=int, required=True, help="Unix-epoch millisecond timestamp to start reading from.")
    context_parser.add_argument(
        "--timeout-ms",
        type=int,
        default=30000,
        help="Consumer timeout in milliseconds when reading existing records.",
    )
    context_parser.add_argument(
        "--max-messages",
        type=int,
        default=100,
        help="Max number of matching records to return from topic tail.",
    )
    context_parser.set_defaults(handler=run_context)

    clear_parser = subparsers.add_parser("clear", help="Recreate a Diaspora topic.")
    clear_parser.add_argument("--topic", default="topic-parsl-local", help="Diaspora topic name without namespace.")
    clear_parser.set_defaults(handler=run_clear)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.handler(args))


if __name__ == "__main__":
    raise SystemExit(main())
