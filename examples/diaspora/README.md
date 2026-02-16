# Diaspora Examples CLI

This directory now uses a single entrypoint CLI:

- `diaspora.py`: unified command surface for setup, workflows, failures, context, and clear.
- `config.py`: shared constants/defaults and local/Aurora Parsl config builders.
- `runtime.py`: shared logging/config/Parsl runtime helpers.
- `workflows.py`: hello-world and Monte Carlo Pi workflows.
- `failures.py`: unified failure scenario engine.
- `topic_ops.py`: setup/context/clear handlers.

## Defaults

- `local`
  - topic: `topic-parsl-local`
  - log file: `parsl-local.log`
- `aurora`
  - topic: `topic-parsl-aurora-debug`
  - log file: `parsl-aurora-debug.log`
  - queue selection by `--count`:
    - `count` 1 or 2 -> `debug`
    - otherwise -> `debug-scaling`

## Usage

```bash
# from repo root
cd examples/diaspora

# one-time setup
python diaspora.py setup

# hello workflow
python diaspora.py hello-world --mode local --count 3
python diaspora.py hello-world --mode aurora --count 3

# Monte Carlo Pi workflow
python diaspora.py monte-carlo --mode local --count 3
python diaspora.py monte-carlo --mode aurora --count 3

# failure scenarios
python diaspora.py failure --scenario python-div-zero --mode local
python diaspora.py failure --scenario python-missing-module --mode local
python diaspora.py failure --scenario python-pep750-t-string --mode local
python diaspora.py failure --scenario python-chain --mode local

# fetch retry context messages (all runs)
python diaspora.py context --mode local
python diaspora.py context --mode aurora

# optionally filter by run_id
python diaspora.py context --mode local --run_id 99f3d61b-58ab-4d8b-a004-bbfd1dbc43b9

# recreate topic + remove file
python diaspora.py clear --mode local
python diaspora.py clear --mode aurora
python diaspora.py clear --topic my-topic --log-file my.log
```
