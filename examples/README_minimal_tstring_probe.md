# Minimal T-String Probe

Script:
- `examples/minimal_tstring_probe.py`
- `examples/config.py` (shared local/Aurora/Midway execution config)

This example is a minimal Parsl workflow with:
- one `python_app` (`pep750_tstring_probe`)
- two retry policies:
  - `budget`: retry-budget based
  - `llm-minimax`: runtime patch retry via MiniMax
- three execution modes:
  - `local`
  - `aurora`
  - `midway` (Slurm)
- monitoring config in all modes (`make_monitoring_config`)

## Prerequisites

From repo root:

```bash
source venv/bin/activate
python -m pip install -e ".[diaspora,monitoring]"
python examples/diaspora/diaspora.py setup
```

For `--retry-policy llm-minimax`, configure your key in `examples/.env`:

```bash
cat > /home/haochenpan/parsl/examples/.env <<'EOF'
MINIMAX_API_KEY="<your-key>"
EOF
```

`minimal_tstring_probe.py` loads `examples/.env` automatically before creating the MiniMax client.

## Usage

From repo root:

```bash
cd /home/haochenpan/parsl/examples
```

Local + classic retry:

```bash
python minimal_tstring_probe.py --config local --retry-policy budget --retries 1
```

Local + LLM retry:

```bash
python minimal_tstring_probe.py --config local --retry-policy llm-minimax --retries 1
```

Aurora:

```bash
python minimal_tstring_probe.py --config aurora --retry-policy budget --retries 1
```

Aurora + LLM retry:

```bash
python minimal_tstring_probe.py --config aurora --retry-policy llm-minimax --minimax-model MiniMax-M2.5 --retries 3
```

Midway3 Slurm:

```bash
python minimal_tstring_probe.py --config midway --retry-policy budget --retries 1
```

Midway3 Slurm + LLM retry:

```bash
python minimal_tstring_probe.py --config midway --retry-policy llm-minimax --retries 3
```

## CLI Options

- `--config {local,aurora,midway}`
- `--retry-policy {budget,llm-minimax}`
- `--retries`
- `--topic`

## Notes

- The app intentionally executes `t"Hello {name}"` dynamically.
- On Python runtimes without t-string support, this raises `SyntaxError` and exercises retry handling.
- Runtime LLM patching is in-memory only; source files are not modified.
- `--config midway` uses `SlurmProvider` + `SrunLauncher` with hard-coded defaults:
  account=`pi-chard`, partition=`caslake`, walltime=`00:10:00`, nodes_per_block=`1`, max_workers_per_node=`1`.
