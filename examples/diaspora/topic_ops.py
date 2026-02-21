"""Topic and setup operations for the diaspora CLI."""

from __future__ import annotations

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
