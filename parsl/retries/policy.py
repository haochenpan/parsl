from __future__ import annotations

import ast
import inspect
import json
import logging
import re
import textwrap
import traceback
import uuid
from typing import Any, Callable, Mapping, Optional, Sequence, Tuple, Type

from parsl.app.errors import wrap_error
from parsl.dataflow.taskrecord import TaskRecord
from parsl.retries.client import LLMRetryClient
from parsl.retries.diaspora_context import fetch_diaspora_context
from parsl.retries.types import RetryDecision, RetryDirective, RetryPatch

logger = logging.getLogger(__name__)

DEFAULT_ALLOWLIST: Tuple[Type[BaseException], ...] = (
    SyntaxError,
    NameError,
    ImportError,
    ModuleNotFoundError,
)


class RetryPolicyError(RuntimeError):
    pass


def _pretty_json(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True, default=str)


def _abort_cost(task_record: TaskRecord) -> float:
    retries = getattr(task_record["dfk"].config, "retries", 0)
    return float(retries + 1)


def _strip_code_fences(text: str) -> str:
    cleaned = text.strip()
    if cleaned.startswith("```") and cleaned.endswith("```"):
        lines = cleaned.splitlines()
        if len(lines) >= 2:
            cleaned = "\n".join(lines[1:-1]).strip()
    return cleaned


def _extract_json_payload(content: str) -> Mapping[str, Any] | None:
    candidates = [content.strip()]
    if "</think>" in content:
        candidates.append(content.split("</think>", 1)[1].strip())

    for candidate in candidates:
        cleaned = _strip_code_fences(candidate)
        try:
            payload = json.loads(cleaned)
        except Exception:
            pass
        else:
            if isinstance(payload, Mapping):
                return payload

        left = cleaned.find("{")
        right = cleaned.rfind("}")
        if left != -1 and right != -1 and right > left:
            maybe_obj = cleaned[left:right + 1]
            try:
                payload = json.loads(maybe_obj)
            except Exception:
                continue
            if isinstance(payload, Mapping):
                return payload

    return None


def _extract_function_from_python_text(text: str, expected_name: str) -> str | None:
    try:
        tree = ast.parse(text, mode="exec")
    except Exception:
        return None

    functions = [node for node in tree.body if isinstance(node, ast.FunctionDef)]
    if not functions:
        return None

    selected = None
    for node in functions:
        if node.name == expected_name:
            selected = node
            break
    if selected is None:
        selected = functions[0]

    segment = ast.get_source_segment(text, selected)
    if segment is not None and segment.strip():
        return segment.strip()

    if selected.end_lineno is not None:
        lines = text.splitlines()
        return "\n".join(lines[selected.lineno - 1:selected.end_lineno]).strip()

    return None


def _extract_function_payload(content: str, expected_name: str) -> Mapping[str, Any] | None:
    fenced_blocks = re.findall(r"```(?:python)?\s*(.*?)```", content, flags=re.IGNORECASE | re.DOTALL)
    candidates = [block.strip() for block in fenced_blocks if block.strip()]
    candidates.append(content.strip())
    if "</think>" in content:
        candidates.append(content.split("</think>", 1)[1].strip())

    for candidate in candidates:
        maybe_source = _extract_function_from_python_text(candidate, expected_name)
        if maybe_source:
            return {
                "patched_function_source": maybe_source,
                "summary": "parsed from non-JSON response",
            }

    return None


def _parse_patch_payload(content: str, expected_name: str) -> Mapping[str, Any]:
    payload = _extract_json_payload(content)
    if payload is None:
        payload = _extract_function_payload(content, expected_name)
    if payload is None:
        raise RetryPolicyError("LLM response could not be parsed as JSON or Python function")

    source = payload.get("patched_function_source")
    if not isinstance(source, str) or not source.strip():
        raise RetryPolicyError("LLM response missing 'patched_function_source'")

    return payload


def _unwrap_for_source(func: Callable) -> Callable:
    try:
        return inspect.unwrap(func)
    except Exception:
        return func


def _compile_replacement_function(
    *,
    expected_name: str,
    patched_function_source: str,
    original_function: Callable,
) -> Callable:
    source = textwrap.dedent(patched_function_source)

    try:
        tree = ast.parse(source, mode="exec")
    except Exception as exc:
        raise RetryPolicyError("Patched function source is not valid Python") from exc

    function_defs = [n for n in tree.body if isinstance(n, ast.FunctionDef)]
    if not function_defs:
        raise RetryPolicyError("Patched source must define a function")

    selected = None
    for fn in function_defs:
        if fn.name == expected_name:
            selected = fn
            break
    if selected is None:
        selected = function_defs[0]

    code = compile(tree, "<retry_llm_patch>", "exec")

    namespace = dict(getattr(original_function, "__globals__", {}))
    local_ns: dict[str, Any] = {}
    exec(code, namespace, local_ns)

    patched = local_ns.get(selected.name) or namespace.get(selected.name)
    if not callable(patched):
        raise RetryPolicyError("Compiled patch did not produce a callable function")

    return patched


def build_retry_llm_policy(
    *,
    llm_client: LLMRetryClient,
    diaspora_topic: str,
    model: str = "MiniMax-M2.5",
    allowlisted_exceptions: Optional[Sequence[Type[BaseException]]] = None,
    max_patch_attempts: int = 1,
    diaspora_timeout_ms: int = 30000,
    diaspora_max_messages: int = 100,
    temperature: float = 0.2,
    environment: Optional[str] = None,
    policy_logger: Optional[logging.Logger] = None,
) -> Callable[[Exception, TaskRecord], RetryDecision]:
    """Build a retry handler that asks an LLM for a patched callable."""

    if max_patch_attempts < 1:
        raise ValueError("max_patch_attempts must be >= 1")

    active_logger = policy_logger or logger
    exception_types = tuple(allowlisted_exceptions or DEFAULT_ALLOWLIST)

    def retry_llm_policy(exception: Exception, task_record: TaskRecord) -> RetryDecision:
        task_id = int(task_record["id"])
        run_id = str(task_record["dfk"].run_id)
        func_name = str(task_record.get("func_name", "<unknown>"))

        if not isinstance(exception, exception_types):
            active_logger.info(
                "retry_llm_policy skipped LLM patch for exception type %s",
                type(exception).__name__,
                extra={
                    "parsl_run_id": run_id,
                    "parsl_task_id": task_id,
                    "parsl_try_id": int(task_record.get("try_id", 0)),
                    "parsl_exception_type": type(exception).__name__,
                    "parsl_retry_llm_skipped": True,
                },
            )
            return 1.0

        patch_history = list(task_record.get("retry_patch_history", []))
        if len(patch_history) >= max_patch_attempts:
            active_logger.warning(
                "retry_llm_policy patch budget exhausted",
                extra={
                    "parsl_run_id": run_id,
                    "parsl_task_id": task_id,
                    "parsl_retry_patch_attempts": len(patch_history),
                },
            )
            return _abort_cost(task_record)

        original_function = _unwrap_for_source(task_record["func"])

        try:
            original_source = inspect.getsource(original_function)
        except Exception as exc:
            active_logger.exception("Could not get source for function %s", func_name)
            task_record["retry_patch_last_error"] = f"source-introspection failed: {exc}"
            return _abort_cost(task_record)

        tb_text = "".join(traceback.format_exception(type(exception), exception, exception.__traceback__))

        try:
            diaspora_context = fetch_diaspora_context(
                topic_name=diaspora_topic,
                run_id=run_id,
                timeout_ms=diaspora_timeout_ms,
                max_messages=diaspora_max_messages,
                environment=environment,
            )
        except Exception as exc:
            active_logger.exception("Failed to gather Diaspora retry context")
            task_record["retry_patch_last_error"] = f"diaspora-context failed: {exc}"
            return _abort_cost(task_record)

        system_prompt = (
            "You are an expert Python assistant that fixes exactly one broken function. "
            "Return strict JSON only with keys: patched_function_source, summary. "
            "Do not include markdown fences."
        )

        user_prompt_payload = {
            "task": "Patch a failed Parsl python_app function for immediate runtime retry.",
            "constraints": [
                "Preserve function name and signature when possible.",
                "Return only valid Python function source.",
                "Do not add external dependencies.",
            ],
            "function_name": func_name,
            "original_function_source": original_source,
            "exception_type": type(exception).__name__,
            "exception_message": str(exception),
            "exception_traceback": tb_text,
            "diaspora_context": diaspora_context,
        }
        user_prompt = _pretty_json(user_prompt_payload)

        active_logger.info(
            "Prepared LLM patch request for task %s\n%s",
            task_id,
            _pretty_json(
                {
                    "parsl_run_id": run_id,
                    "parsl_task_id": task_id,
                    "parsl_try_id": int(task_record.get("try_id", 0)),
                    "parsl_exception_type": type(exception).__name__,
                    "parsl_retry_patch_model": model,
                    "system_prompt": system_prompt,
                    "user_prompt": user_prompt_payload,
                }
            ),
            extra={
                "parsl_run_id": run_id,
                "parsl_task_id": task_id,
                "parsl_try_id": int(task_record.get("try_id", 0)),
                "parsl_exception_type": type(exception).__name__,
                "parsl_retry_patch_model": model,
            },
        )

        try:
            llm_content = llm_client.generate_patch(
                model=model,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=temperature,
            )
            payload = _parse_patch_payload(llm_content, func_name)
            patched_function_source = str(payload["patched_function_source"])
            patched_raw = _compile_replacement_function(
                expected_name=func_name,
                patched_function_source=patched_function_source,
                original_function=original_function,
            )
            patched_wrapped = wrap_error(patched_raw)
        except Exception as exc:
            active_logger.exception("LLM patch generation failed")
            task_record["retry_patch_last_error"] = f"llm-patch failed: {exc}"
            return _abort_cost(task_record)

        patch_id = str(uuid.uuid4())
        metadata = {
            "model": model,
            "summary": payload.get("summary"),
            "exception_type": type(exception).__name__,
            "diaspora_matched_count": diaspora_context.get("matched_count", 0),
        }

        active_logger.info(
            (
                "Generated runtime patch for task %s\n%s"
            ),
            task_id,
            _pretty_json(
                {
                    "parsl_run_id": run_id,
                    "parsl_task_id": task_id,
                    "parsl_try_id": int(task_record.get("try_id", 0)),
                    "parsl_exception_type": type(exception).__name__,
                    "parsl_retry_patch_id": patch_id,
                    "parsl_retry_patch_model": model,
                    "retry_patch_metadata": metadata,
                    "patched_function_source": patched_function_source,
                }
            ),
            extra={
                "parsl_run_id": run_id,
                "parsl_task_id": task_id,
                "parsl_try_id": int(task_record.get("try_id", 0)),
                "parsl_exception_type": type(exception).__name__,
                "parsl_retry_patch_id": patch_id,
                "parsl_retry_patch_model": model,
            },
        )

        return RetryDirective(
            cost=1.0,
            patch=RetryPatch(func=patched_wrapped, patch_id=patch_id, metadata=metadata),
            reason="llm_patch_generated",
        )

    return retry_llm_policy
