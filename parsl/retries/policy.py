from __future__ import annotations

import inspect
import json
import logging
import platform
import traceback
import uuid
from typing import Any, Callable, Optional, Sequence, Tuple, Type

from parsl.app.errors import wrap_error
from parsl.dataflow.taskrecord import TaskRecord
from parsl.retries.client import LLMRetryClient
from parsl.retries.diaspora_context import fetch_diaspora_context
from parsl.retries.parsing import (
    compile_replacement_function,
    parse_patch_payload,
    unwrap_for_source,
)
from parsl.retries.types import RetryDecision, RetryDirective, RetryPatch

logger = logging.getLogger(__name__)

DEFAULT_ALLOWLIST: Tuple[Type[BaseException], ...] = (
    SyntaxError,
    NameError,
    ImportError,
    ModuleNotFoundError,
)


def _pretty_json(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True, default=str)


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
    abort_cost: Optional[float] = None,
    policy_logger: Optional[logging.Logger] = None,
) -> Callable[[Exception, TaskRecord], RetryDecision]:
    """Build a retry handler that asks an LLM for a patched callable.

    Args:
        abort_cost: Cost to assign when the policy gives up (e.g. diaspora
            failure, source introspection failure, budget exhausted). When
            ``None`` (default), falls back to ``config.retries + 1`` which
            ensures no further retries.
    """

    if max_patch_attempts < 1:
        raise ValueError("max_patch_attempts must be >= 1")

    active_logger = policy_logger or logger
    exception_types = tuple(allowlisted_exceptions or DEFAULT_ALLOWLIST)

    def _get_abort_cost(task_record: TaskRecord) -> float:
        if abort_cost is not None:
            return abort_cost
        retries = getattr(task_record["dfk"].config, "retries", 0)
        return float(retries + 1)

    def retry_llm_policy(exception: Exception, task_record: TaskRecord) -> RetryDecision:
        task_id = int(task_record["id"])
        run_id = str(task_record["dfk"].run_id)
        try_id = int(task_record.get("try_id", 0))
        func_name = str(task_record.get("func_name", "<unknown>"))
        exception_type = type(exception).__name__
        tb_text = "".join(traceback.format_exception(type(exception), exception, exception.__traceback__))

        log_extra = {
            "parsl_run_id": run_id,
            "parsl_task_id": task_id,
            "parsl_try_id": try_id,
            "parsl_func_name": func_name,
            "parsl_exception_type": exception_type,
        }

        if not isinstance(exception, exception_types):
            active_logger.info(
                "retry_llm_policy skipped LLM patch for non-allowlisted exception type %s",
                exception_type,
                extra={**log_extra, "parsl_retry_llm_skipped": True},
            )
            return 1.0

        active_logger.info(
            "retry_llm_policy handling %s for task %s (try %s)",
            exception_type,
            task_id,
            try_id,
            extra={
                **log_extra,
                "parsl_exception_message": str(exception),
                "parsl_exception_traceback": tb_text,
                "parsl_fail_count": int(task_record.get("fail_count", 0)),
            },
        )

        patch_history = list(task_record.get("retry_patch_history", []))
        if len(patch_history) >= max_patch_attempts:
            active_logger.warning(
                "retry_llm_policy patch budget exhausted (%s/%s)",
                len(patch_history),
                max_patch_attempts,
                extra=log_extra,
            )
            return _get_abort_cost(task_record)

        original_function = unwrap_for_source(task_record["func"])

        try:
            original_source = inspect.getsource(original_function)
        except Exception as exc:
            active_logger.exception("Could not get source for function %s", func_name, extra=log_extra)
            task_record["retry_patch_last_error"] = f"source-introspection failed: {exc}"
            return _get_abort_cost(task_record)

        # Strip decorator lines so the LLM doesn't echo them back
        source_lines = original_source.splitlines()
        while source_lines and source_lines[0].lstrip().startswith("@"):
            source_lines.pop(0)
        original_source = "\n".join(source_lines)

        try:
            diaspora_context = fetch_diaspora_context(
                topic_name=diaspora_topic,
                run_id=run_id,
                timeout_ms=diaspora_timeout_ms,
                max_messages=diaspora_max_messages,
                environment=environment,
            )
        except Exception as exc:
            active_logger.exception("Failed to gather Diaspora retry context", extra=log_extra)
            task_record["retry_patch_last_error"] = f"diaspora-context failed: {exc}"
            return _get_abort_cost(task_record)

        system_prompt = (
            "Fix the broken Python function. "
            "Return JSON with keys: patched_function_source, summary. "
            "patched_function_source must contain ONLY the bare function definition "
            "(def ...), without any decorators such as @python_app. "
            "No markdown fences."
        )

        user_prompt_payload = {
            "function_name": func_name,
            "original_function_source": original_source,
            "python_version": platform.python_version(),
            "exception_type": exception_type,
            "exception_message": str(exception),
            "exception_traceback": tb_text,
            "diaspora_context": diaspora_context,
        }
        user_prompt = _pretty_json(user_prompt_payload)

        active_logger.info(
            "Sending LLM patch request for task %s",
            task_id,
            extra={**log_extra, "parsl_retry_patch_model": model},
        )

        try:
            llm_content = llm_client.generate_patch(
                model=model,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=temperature,
            )
            payload = parse_patch_payload(llm_content, func_name)
            patched_function_source = str(payload["patched_function_source"])
            patched_raw = compile_replacement_function(
                expected_name=func_name,
                patched_function_source=patched_function_source,
                original_function=original_function,
            )
            patched_wrapped = wrap_error(patched_raw)
        except Exception as exc:
            active_logger.exception("LLM patch generation failed", extra=log_extra)
            task_record["retry_patch_last_error"] = f"llm-patch failed: {exc}"
            return _get_abort_cost(task_record)

        patch_id = str(uuid.uuid4())
        metadata = {
            "model": model,
            "summary": payload.get("summary"),
            "exception_type": exception_type,
            "diaspora_matched_count": diaspora_context.get("matched_count", 0),
        }

        active_logger.info(
            "Generated runtime patch %s for task %s",
            patch_id,
            task_id,
            extra={
                **log_extra,
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
