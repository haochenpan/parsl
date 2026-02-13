#!/usr/bin/env python3
"""Recreate a Diaspora topic and remove a local log file.

Usage:
    python examples/diaspora/clear_topic_and_file.py [--mode MODE] [--topic TOPIC] [--log-file LOG_FILE]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from diaspora_event_sdk import Client
from config import (
    AURORA_MODE,
    LOCAL_MODE,
    defaults_for_mode,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=[LOCAL_MODE, AURORA_MODE],
        default=LOCAL_MODE,
        help="Default topic/log-file profile to use when args are omitted.",
    )
    parser.add_argument("--topic", default=None, help="Diaspora topic name without namespace.")
    parser.add_argument("--log-file", default=None, help="Path to local log file to delete.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
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


if __name__ == "__main__":
    raise SystemExit(main())
