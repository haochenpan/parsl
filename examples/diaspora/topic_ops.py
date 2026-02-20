"""Topic and setup operations for the diaspora CLI."""

from __future__ import annotations

import json
import sys

from config import defaults_for_mode


def run_setup(args) -> int:
    try:
        from diaspora_event_sdk import Client as GlobusClient
    except ImportError:
        print(
            "diaspora-event-sdk is not installed. Install with: pip install -e '.[diaspora]'",
            file=sys.stderr,
        )
        return 2

    client = GlobusClient(environment=args.environment)
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

    default_topic, _ = defaults_for_mode(args.mode)
    topic = args.topic or default_topic

    context = fetch_diaspora_context(
        topic_name=topic,
        time_horizon=args.time_horizon,
        timeout_ms=args.timeout_ms,
        max_messages=args.max_messages,
        environment=args.environment,
    )
    print(json.dumps(context, indent=2, default=str))
    return 0


def run_clear(args) -> int:
    from pathlib import Path

    from diaspora_event_sdk import Client

    default_topic, default_log_file = defaults_for_mode(args.mode)
    topic = args.topic or default_topic
    log_file = args.log_file or default_log_file

    client = Client()
    topic_result = client.recreate_topic(topic)
    print(json.dumps(topic_result, indent=2, default=str))

    status = topic_result.get("status")
    if status not in {"success", "no-op"}:
        print(
            f"recreate_topic({topic!r}) returned status={status!r}",
            file=sys.stderr,
        )
        return 1

    log_path = Path(log_file)
    if log_path.exists():
        log_path.unlink()
        print(f"Removed file: {log_path}")
    else:
        print(f"File not found (nothing to remove): {log_path}")

    return 0
