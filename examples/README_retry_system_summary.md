# LLM-Powered Retry System — Change Summary

Summary of changes from commit `0a74364c` to HEAD on the `parsl-diaspora` branch.

## What Changed

A new **LLM-powered retry system** was added to Parsl. When a task fails with an
allowlisted exception (e.g. `SyntaxError`, `NameError`), the retry handler calls an
LLM to generate a **patched version of the function**, hot-swaps it at runtime, and
re-executes — all without modifying source files.

## Architecture — Call Path

```
User Config
  Config(retry_handler=build_retry_llm_policy(...))
       │
       ▼
 ┌─────────────────────────────────────────────────────────┐
 │  DataFlowKernel.handle_exec_update()  (dflow.py)        │
 │                                                         │
 │  on task failure:                                        │
 │    1. retry_handler(exception, task_record)              │
 │         │                                               │
 │         │  returns RetryDecision (float | RetryDirective)│
 │         ▼                                               │
 │    2. _normalize_retry_decision(decision)                │
 │         → always produces RetryDirective(cost, patch?)   │
 │                                                         │
 │    3. if directive.patch:                                │
 │         _apply_retry_patch(task_record, patch)           │
 │         ├─ validates callable is distinct                │
 │         ├─ serialization preflight via serialize()       │
 │         ├─ swaps task_record["func"]                     │
 │         └─ appends to retry_patch_history                │
 │                                                         │
 │    4. task_record['fail_cost'] += directive.cost         │
 └─────────────────────────────────────────────────────────┘

 ┌─────────────────────────────────────────────────────────┐
 │  build_retry_llm_policy()  (retries/policy.py)          │
 │  Returns a closure: retry_llm_policy(exc, task_record)  │
 │                                                         │
 │    1. Check if exception is in allowlist                 │
 │       └─ if not → return 1.0 (simple cost)              │
 │                                                         │
 │    2. Log diagnostics (exception, traceback, task info)  │
 │       via policy_logger using structured extra fields    │
 │                                                         │
 │    3. Check patch budget (max_patch_attempts)            │
 │       └─ if exhausted → return abort_cost               │
 │                                                         │
 │    4. Get original function source via inspect           │
 │                                                         │
 │    5. fetch_diaspora_context()                           │
 │       (retries/diaspora_context.py)                     │
 │       └─ Seeks to tail of Kafka topic partitions        │
 │       └─ Single poll to read records                    │
 │       └─ Filters events by run_id, returns matched list │
 │                                                         │
 │    6. Call llm_client.generate_patch()                   │
 │       └─ LLMRetryClient protocol (retries/client.py)    │
 │       └─ MiniMaxOpenAICompatClient (retries/minimax.py) │
 │           uses openai.OpenAI for any compatible API     │
 │                                                         │
 │    7. parse_patch_payload(llm_content)                   │
 │       (retries/parsing.py)                              │
 │       ├─ extract_json_payload() — JSON parsing           │
 │       └─ extract_function_payload() — fenced Python      │
 │                                                         │
 │    8. compile_replacement_function()                     │
 │       (retries/parsing.py)                              │
 │       └─ ast.parse → compile → exec in original globals │
 │                                                         │
 │    9. wrap_error(patched_raw) → RetryDirective           │
 └─────────────────────────────────────────────────────────┘

 ┌──────────────────────────────────────────────────────────┐
 │  Types  (retries/types.py)                               │
 │                                                         │
 │   RetryPatch(func, patch_id, metadata)                  │
 │   RetryDirective(cost, patch?, reason?)                  │
 │   RetryDecision = Union[float, RetryDirective]           │
 └──────────────────────────────────────────────────────────┘
```

## Files Added/Modified

| File | Change |
|------|--------|
| `parsl/retries/__init__.py` | New package — re-exports all public names |
| `parsl/retries/types.py` | `RetryPatch`, `RetryDirective`, `RetryDecision` dataclasses |
| `parsl/retries/client.py` | `LLMRetryClient` Protocol |
| `parsl/retries/minimax.py` | `MiniMaxOpenAICompatClient` — wraps `openai.OpenAI` for any compatible API |
| `parsl/retries/policy.py` | `build_retry_llm_policy()` — policy builder with structured logging |
| `parsl/retries/parsing.py` | LLM response parsing and function compilation helpers |
| `parsl/retries/diaspora_context.py` | `fetch_diaspora_context()` — Kafka consumer for Diaspora events |
| `parsl/dataflow/dflow.py` | `_normalize_retry_decision`, `_apply_retry_patch`; modified `handle_exec_update` and task record init |
| `parsl/dataflow/taskrecord.py` | 3 new fields: `retry_patch_applied`, `retry_patch_last_error`, `retry_patch_history` |
| `parsl/__init__.py` | Exports 6 new public symbols |
| `parsl/config.py` | Updated `retry_handler` docstring to mention `RetryDirective` |
| `docs/userguide/workflows/exceptions.rst` | Documents RetryDirective usage |
| `parsl/tests/test_error_handling/test_retry_directive.py` | Tests for patch application in DFK |
| `parsl/tests/test_error_handling/test_retry_llm_policy.py` | Tests for policy builder |
| `parsl/tests/test_utils/test_minimax_client.py` | Tests for MiniMax client (openai-based) |
| `parsl/tests/test_utils/test_diaspora_context.py` | Tests for `_collect_tail_records` with mock consumers |
| `examples/diaspora/` | Full example suite (config, workflows, failures, runtime, topic_ops) |
| `examples/minimal_tstring_probe.py` | Standalone tstring probe example |
| `parsl/retries/README.md` | Documentation for the retry subsystem |

## Refactoring Applied

### R1. Split `policy.py` into policy + parsing modules

`policy.py` previously contained three concerns in 344 lines: LLM response parsing,
function compilation, and the policy builder. The parsing and compilation helpers have
been extracted into `retries/parsing.py`:

- `strip_code_fences()` — removes markdown code fences
- `extract_json_payload()` — parses JSON from LLM responses (handles `<think>` tags, fenced blocks, embedded JSON objects)
- `extract_function_payload()` — parses bare Python function definitions from fenced code blocks
- `parse_patch_payload()` — top-level dispatcher: tries JSON first, then Python extraction
- `_extract_function_from_python_text()` — AST-based single-function extraction
- `unwrap_for_source()` — safely unwraps decorated functions for `inspect.getsource`
- `compile_replacement_function()` — compiles patched source and executes in the original function's global namespace
- `RetryPolicyError` — exception class for parsing/compilation failures

`policy.py` now only contains the policy builder, logging, and the allowlist
configuration. It imports from `parsing.py`.

### R2. Removed `_abort_cost` DFK coupling

The old `_abort_cost(task_record)` function reached through
`task_record["dfk"].config.retries` — deep coupling from the retry policy module
into DFK internals.

`build_retry_llm_policy` now accepts an optional `abort_cost: float` parameter.
When provided, this fixed value is used. When `None` (the default), it falls back to
`config.retries + 1` for backward compatibility. The `_abort_cost` top-level function
has been replaced by a `_get_abort_cost` closure inside the policy builder.

### R3. Simplified log context building

Replaced the nested `_context_json(**fields)` closure that built merged JSON strings
on every log call. The policy now uses Python's `logging` structured `extra=` dict
parameter with a base `log_extra` dict that gets spread into each log call:

```python
active_logger.info("...", extra={**log_extra, "parsl_retry_patch_model": model})
```

This is idiomatic Python logging and works with structured log handlers (JSON formatters,
log aggregators) out of the box.

### R4. Removed `_log_retry_diagnostic` from dflow.py

`dflow.py` had a `_log_retry_diagnostic` method that logged exception/traceback/task
metadata on every failure, duplicating what the retry policy already logs. The policy's
`retry_llm_policy` closure now logs diagnostics (exception type, message, traceback,
fail count) via structured `extra=` fields on the first log call for each invocation.

Removed the `traceback` import from `dflow.py` as it was only used by the deleted method.

### R5. Deleted dead filter functions in `diaspora_context.py`

Removed three functions that were defined but never called:
- `_task_matches(event, task_id)` — matched events by task ID
- `_run_matches(event, run_id)` — matched events by run ID
- `_retry_relevant(event)` — checked if an event was retry-related

These were leftover from an earlier iteration. `fetch_diaspora_context()` does its own
simpler `run_id` filtering inline. Also removed the now-unused `Mapping` import.

### R6. Replaced raw HTTP with `openai` library in `MiniMaxOpenAICompatClient`

The client previously hand-crafted HTTP requests using `requests.Session` (~50 lines of
plumbing: headers, status checks, JSON parsing, error wrapping). Since MiniMax exposes
an OpenAI-compatible API, the client now wraps `openai.OpenAI(base_url=..., api_key=...)`.

The constructor accepts an optional `client` parameter (instead of the old `session`)
for dependency injection in tests. The `LLMRetryClient` protocol is unchanged.

### R7. Removed unused `RetryPatchHistoryEntry` type alias

Removed from `types.py`:
```python
RetryPatchHistoryEntry = Dict[str, Any]
```
This alias was defined but never imported or used anywhere in the codebase.

### R8. Simplified Kafka polling loop in `_collect_tail_records`

The old implementation had a complex triple-condition polling loop: tracking
`empty_polls`, a `deadline` timer, and a `caught_up` check across all partitions.
It polled in 200ms increments until all three conditions were met.

The new implementation:
1. Waits for partition assignment (same as before)
2. Seeks each partition to `max(beginning, end - max_messages)`
3. Does a **single `poll()`** with the full timeout
4. Sorts and trims

The offset logging was also changed from `INFO` to `DEBUG` level to reduce noise.

Added 6 unit tests in `test_diaspora_context.py` covering: empty consumer, normal
reads, max_messages limiting, zero max_messages, no-assignment consumer, and
timestamp-based sort ordering.
