# Parsl Retry Patching (LLM + Directive) Design Notes

This README documents the retry-system changes that add runtime patching support while keeping the old float-only retry behavior backward compatible.

## Scope

This feature adds:
1. A new retry decision type (`RetryDirective`) that can carry both retry cost and a runtime callable patch.
2. A provider-agnostic LLM retry policy builder (`build_retry_llm_policy`).
3. A MiniMax OpenAI-compatible client using `requests`.
4. DFK support for applying validated runtime patches before retry.
5. Extra retry diagnostics in structured logs.

## Key Files

- `parsl/retries/types.py`: new retry types (`RetryDecision`, `RetryDirective`, `RetryPatch`).
- `parsl/retries/client.py`: LLM client protocol.
- `parsl/retries/minimax.py`: MiniMax OpenAI-compatible client implementation.
- `parsl/retries/diaspora_context.py`: Diaspora topic context fetcher.
- `parsl/retries/policy.py`: `build_retry_llm_policy` implementation.
- `parsl/dataflow/dflow.py`: retry decision normalization and patch application.
- `parsl/config.py`: retry handler typing widened to decision union.
- `parsl/dataflow/taskrecord.py`: patch bookkeeping fields.
- `examples/minimal_tstring_probe.py`: demo wiring with `once` and `llm-minimax`.

## Public API Changes

1. `Config.retry_handler` now accepts `Callable[[Exception, TaskRecord], RetryDecision]`:
   - `parsl/config.py:102`
2. New retry datatypes:
   - `RetryPatch`: `parsl/retries/types.py:7`
   - `RetryDirective`: `parsl/retries/types.py:16`
   - `RetryDecision = Union[float, RetryDirective]`: `parsl/retries/types.py:25`
3. New policy entrypoint:
   - `build_retry_llm_policy(...)`: `parsl/retries/policy.py:182`
4. New MiniMax client:
   - `MiniMaxOpenAICompatClient`: `parsl/retries/minimax.py:9`
5. Re-exported via:
   - `parsl/retries/__init__.py:1`
   - `parsl/__init__.py:32`

## Old Call Path (Pre-Directive, Float Cost Only)

This is still supported.

1. App failure reaches DFK callback:
   - `DataFlowKernel.handle_exec_update`: `parsl/dataflow/dflow.py:374`
2. Exception is captured and retry handler is called (if configured):
   - `retry_decision = self._config.retry_handler(e, task_record)`: `parsl/dataflow/dflow.py:406`
3. Retry cost is added to `task_record['fail_cost']`.
4. If `fail_cost <= config.retries`, task returns to pending and is relaunched.

Legacy semantics are unchanged when handler returns a `float`.

## New Call Path (Directive + Runtime Patch)

DFK now supports either `float` or `RetryDirective`.

1. Failure is captured and logged with retry diagnostics:
   - `_log_retry_diagnostic(...)`: `parsl/dataflow/dflow.py:306`
2. Retry decision is normalized:
   - `_normalize_retry_decision(...)`: `parsl/dataflow/dflow.py:323`
   - `float` becomes `RetryDirective(cost=float_value)`
3. If directive contains `patch`, DFK validates and applies it:
   - `_apply_retry_patch(...)`: `parsl/dataflow/dflow.py:335`
4. Patch validation rules:
   - patch callable required: `parsl/dataflow/dflow.py:339`
   - must be distinct function object: `parsl/dataflow/dflow.py:344`
   - serialization preflight: `parsl/dataflow/dflow.py:347`
5. Task bookkeeping updated:
   - `retry_patch_applied`, `retry_patch_last_error`, `retry_patch_history`
   - initialization: `parsl/dataflow/dflow.py:1036`
   - typed fields: `parsl/dataflow/taskrecord.py:49`
6. Retry scheduling proceeds through existing DFK state logic:
   - retry transition: `parsl/dataflow/dflow.py:430`

## LLM Policy Call Path

When using `build_retry_llm_policy(...)`:

1. Exception type gate (default allowlist: syntax/name/import class errors):
   - `parsl/retries/policy.py:208`
2. Patch budget gate:
   - `parsl/retries/policy.py:223`
3. Source extraction for original function:
   - `parsl/retries/policy.py:237`
4. Diaspora context fetch:
   - `fetch_diaspora_context(...)`: `parsl/retries/policy.py:246`
   - implementation: `parsl/retries/diaspora_context.py:63`
5. LLM request:
   - `llm_client.generate_patch(...)`: `parsl/retries/policy.py:286`
6. Parse/validate payload and compile replacement function:
   - payload parse: `parsl/retries/policy.py:292`
   - compile: `parsl/retries/policy.py:294`
7. Wrap with `wrap_error(...)` for python_app parity:
   - `parsl/retries/policy.py:299`
8. Return directive with patch and retry cost:
   - `parsl/retries/policy.py:343`
9. On failures in policy generation path, fail closed:
   - return abort cost: `parsl/retries/policy.py:303`

## MiniMax Client Call Path

1. Build client:
   - `MiniMaxOpenAICompatClient(...)`: `parsl/retries/minimax.py:12`
2. Env var auth:
   - requires `MINIMAX_API_KEY`: `parsl/retries/minimax.py:20`
3. Request format:
   - endpoint: `/v1/chat/completions`: `parsl/retries/minimax.py:39`
   - `n=1`: `parsl/retries/minimax.py:51`
   - `temperature in (0,1]`: `parsl/retries/minimax.py:36`
4. Response parse:
   - `choices[0].message.content`: `parsl/retries/minimax.py:65`

## Logging Path (Patch Generation Event)

Patch generation now logs detailed payload content in stream logs:
- message: `"Generated runtime patch for task %s details=%s"`
- code: `parsl/retries/policy.py:313`
- structured extras still included (`parsl_run_id`, `parsl_task_id`, `parsl_try_id`, `parsl_exception_type`, patch id/model).

## Example Workflow Path (`minimal_tstring_probe.py`)

Current example supports both old and new retry modes:

1. `--retry-policy once`:
   - classic cost-based retry, now parameterized by `--retries`
   - policy builder: `examples/minimal_tstring_probe.py:48`
2. `--retry-policy llm-minimax`:
   - runtime patching flow through `build_retry_llm_policy(...)`
   - handler build: `examples/minimal_tstring_probe.py:59`
3. `--retries` controls Parsl retry budget:
   - CLI arg: `examples/minimal_tstring_probe.py:221`
   - config wiring: `examples/minimal_tstring_probe.py:260`

## Behavioral Notes

1. Runtime patching is in-memory only. No source file write-back is performed.
2. Existing float-returning retry handlers continue to work unchanged.
3. If retry handler/policy throws, DFK aborts retries for that task (existing behavior preserved).
4. If an LLM patch produces code that returns nested app futures, follow-on tasks can be created; this is normal Parsl behavior but may be surprising in the example output.
