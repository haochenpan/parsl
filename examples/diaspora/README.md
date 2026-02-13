# Diaspora Examples

This folder contains small Parsl + Diaspora utilities for local and Aurora runs.

## Files

- `diaspora_setup.py`: one-time Diaspora user bootstrap (`create_user()`).
- `diaspora_run.py`: main hello-world workflow runner with Diaspora logging.
- `diaspora_run_ensemble.py`: ensemble + analysis workflow with richer task logs.
- `diaspora_run_failure1.py`: fail-once then retry-success for both `python_app` and `bash_app`.
- `diaspora_run_failure2.py`: timeout failures (`AppTimeout`) then retry-success for both app types.
- `diaspora_run_failure3.py`: zero-division + missing-output failures then retry-success.
- `consume_topic.py`: consumes a topic and prints events containing `"python_app"`.
- `clear_topic_and_file.py`: recreates a Diaspora topic and removes a local log file.
- `config.py`: shared constants/defaults plus local/Aurora Parsl config builders and common run-argument parser.
- `util.py`: shared helpers for topic/log-file resolution and metrics.
- `monitoring_radios_loggers_architecture.md`: deep code map of monitoring radios/loggers, event flow, communication diagrams, and submitter-visible behavior.

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

`diaspora_run.py`, `diaspora_run_ensemble.py`, and `diaspora_run_failure*.py` share:

- `--mode {local,aurora}`
- `--topic TOPIC`
- `--count COUNT`
- `--log-file LOG_FILE`
- `--log-level LOG_LEVEL`

## Quick Usage

```bash
# from repo root
cd examples/diaspora

# 1) one-time setup (no mode)
python diaspora_setup.py

# 2) diaspora_run.py
python diaspora_run.py --mode local --count 3
python diaspora_run.py --mode aurora --count 3

# 3) diaspora_run_ensemble.py
python diaspora_run_ensemble.py --mode local --count 3
python diaspora_run_ensemble.py --mode aurora --count 3

# 4) consume_topic.py
python consume_topic.py --mode local
python consume_topic.py --mode aurora

# 5) failure demos
python diaspora_run_failure1.py --mode local --count 1
python diaspora_run_failure2.py --mode local --count 1
python diaspora_run_failure3.py --mode local --count 1

# 6) clear_topic_and_file.py
python clear_topic_and_file.py --mode local
python clear_topic_and_file.py --mode aurora

# optional: reset specific topic/file
python clear_topic_and_file.py --topic my-topic --log-file my.log
```
