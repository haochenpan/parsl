# Minimal T-String Probe Example

Script:
- `examples/minimal_tstring_probe.py`

This example runs one Parsl `python_app` with:
- stream + Diaspora logging at `DEBUG`
- monitoring enabled in both modes
- configurable retry policy:
  - `once`: retry budget accounting only
  - `llm-minimax`: runtime patch generation via MiniMax M2.5
- configurable retry budget via `--retries`
- detailed patch diagnostics in stream logs when LLM patching is used

## Prerequisites

From repo root:

```bash
source venv/bin/activate
python -m pip install -e ".[diaspora,monitoring]"
python examples/diaspora/diaspora.py setup
```

Note: keep the virtual environment activated for Aurora mode so HTEX helper scripts are available on `PATH`.

## MiniMax LLM Retry Setup

For `--retry-policy llm-minimax`, export your MiniMax API key:

```bash
export MINIMAX_API_KEY="<your-key>"
```

The example uses the OpenAI-compatible MiniMax endpoint through `requests`.

## Usage

Run in local mode (classic retry):

```bash
python examples/minimal_tstring_probe.py --mode local --retry-policy once --retries 1
```

Run in local mode (LLM patch retry):

```bash
python examples/minimal_tstring_probe.py \
  --mode local \
  --retry-policy llm-minimax \
  --minimax-model MiniMax-M2.5 \
  --retries 1
```

Run in Aurora mode:

```bash
python examples/minimal_tstring_probe.py --mode aurora --retry-policy once --retries 1
```

Override topic:

```bash
python examples/minimal_tstring_probe.py --mode local --topic topic-parsl-local
```

## Defaults

- local
  - topic: `topic-parsl-local`
  - monitoring interval: `0.5s`
  - executor: `ThreadPoolExecutor(max_threads=1)`
- aurora
  - topic: `topic-parsl-aurora-debug`
  - queue: `debug`
  - nodes per block: `1`
  - monitoring interval: `10s`
- LLM retry
  - model: `MiniMax-M2.5`
  - trigger exceptions: `SyntaxError`, `NameError`, `ImportError`, `ModuleNotFoundError`
  - patch generation log line includes JSON details:
    - `Generated runtime patch for task ... details={...}`
- retries
  - default retry budget: `1` (settable with `--retries`)

## CLI Options

- `--mode {local,aurora}`: execution backend.
- `--topic`: optional Diaspora topic name.
- `--retry-policy {once,llm-minimax}`: retry behavior.
- `--minimax-model`: model for MiniMax OpenAI-compatible API.
- `--retries`: retry budget/count passed to Parsl `Config.retries`.

## Notes

- Runtime patching is in-memory only. Source files are not modified.
- Keep API keys out of shell history and rotate any key that was exposed.
