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