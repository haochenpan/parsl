# Diaspora Examples

This folder contains small Parsl + Diaspora utilities for local and Aurora runs.

## Files

- `diaspora_setup.py`: one-time Diaspora user bootstrap (`create_user()`).
- `diaspora_run.py`: main hello-world workflow runner with Diaspora logging.
- `diaspora_run_ensemble.py`: ensemble + analysis workflow with richer task logs.
- `consume_topic.py`: consumes a topic and prints events containing `"python_app"`.
- `clear_topic_and_file.py`: recreates a Diaspora topic and removes a local log file.
- `config.py`: shared constants/defaults plus local/Aurora Parsl config builders and common run-argument parser.
- `util.py`: shared helpers for topic/log-file resolution and metrics.

## Modes and Defaults

- `local`
  - topic: `topic-parsl-local`
  - log file: `parsl-local.log`
- `aurora`
  - topic: `topic-parsl-aurora-debug`
  - log file: `parsl-aurora-debug.log`
  - queue selection still depends on `--count`:
    - `count` 1 or 2 -> `debug`
    - otherwise -> `debug-scaling`

## Common Run Args

`diaspora_run.py` and `diaspora_run_ensemble.py` share:

- `--mode {local,aurora}`
- `--topic TOPIC`
- `--count COUNT`
- `--log-file LOG_FILE`
- `--log-level LOG_LEVEL`

## Quick Usage

```bash
# 1) one-time setup
python examples/diaspora/diaspora_setup.py

# 2) run locally (default mode)
python examples/diaspora/diaspora_run.py

# 3) run ensemble locally
python examples/diaspora/diaspora_run_ensemble.py --count 3

# 4) run on Aurora
python examples/diaspora/diaspora_run.py --mode aurora --count 3

# 5) consume only events that contain "python_app"
python examples/diaspora/consume_topic.py --mode aurora

# 6) reset topic + remove log file (mode defaults)
python examples/diaspora/clear_topic_and_file.py --mode aurora

# 7) reset specific topic/file
python examples/diaspora/clear_topic_and_file.py --topic my-topic --log-file my.log
```
