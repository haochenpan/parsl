#!/usr/bin/env python3
"""Unified CLI for Parsl Diaspora examples.

Install dependencies from this checkout with:
    pip install -e ".[diaspora,monitoring]"
"""

from __future__ import annotations

import argparse
import json
import sys


def run_setup(_args) -> int:
    from diaspora_event_sdk import Client

    client = Client()
    result = client.create_user()

    print(json.dumps(result, indent=2, default=str))

    status = result.get("status")
    if status not in {"success", "no-op"}:
        print(
            f"create_user() returned status={status!r}; setup may be incomplete.",
            file=sys.stderr,
        )
        return 1

    print("Diaspora user setup complete.")
    return 0


def run_context(args) -> int:
    from parsl.retries.diaspora_context import fetch_diaspora_context

    context = fetch_diaspora_context(
        topic_name=args.topic,
        time_horizon=args.time_horizon,
        timeout_ms=args.timeout_ms,
        max_messages=args.max_messages,
        environment=None,
    )
    print(json.dumps(context, indent=2, default=str))
    return 0


def run_clear(args) -> int:
    from diaspora_event_sdk import Client

    client = Client()
    topic_result = client.recreate_topic(args.topic)
    print(json.dumps(topic_result, indent=2, default=str))

    status = topic_result.get("status")
    if status not in {"success", "no-op"}:
        print(
            f"recreate_topic({args.topic!r}) returned status={status!r}",
            file=sys.stderr,
        )
        return 1

    return 0


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
