#!/usr/bin/env python3
"""One-time Diaspora auth bootstrap.

This script initializes the Diaspora SDK client and calls ``create_user()``
once to complete the user-side setup/auth workflow.
"""

import argparse
import json
import sys


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bootstrap Diaspora user setup once.")
    parser.add_argument(
        "--environment",
        default=None,
        help="Optional Diaspora SDK environment override (for example: local).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        from diaspora_event_sdk import Client as GlobusClient
    except ImportError:
        print(
            "diaspora-event-sdk is not installed. "
            "Install with: pip install -e '.[diaspora]'",
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


if __name__ == "__main__":
    raise SystemExit(main())
