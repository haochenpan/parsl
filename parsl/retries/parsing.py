from __future__ import annotations

import ast
import inspect
import json
import re
import textwrap
from typing import Any, Callable, Mapping


class RetryPolicyError(RuntimeError):
    pass


def strip_code_fences(text: str) -> str:
    cleaned = text.strip()
    if cleaned.startswith("```") and cleaned.endswith("```"):
        lines = cleaned.splitlines()
        if len(lines) >= 2:
            cleaned = "\n".join(lines[1:-1]).strip()
    return cleaned


def extract_json_payload(content: str) -> Mapping[str, Any] | None:
    candidates = [content.strip()]
    if "</think>" in content:
        candidates.append(content.split("</think>", 1)[1].strip())

    for candidate in candidates:
        cleaned = strip_code_fences(candidate)
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


def extract_function_payload(content: str, expected_name: str) -> Mapping[str, Any] | None:
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


def parse_patch_payload(content: str, expected_name: str) -> Mapping[str, Any]:
    payload = extract_json_payload(content)
    if payload is None:
        payload = extract_function_payload(content, expected_name)
    if payload is None:
        raise RetryPolicyError("LLM response could not be parsed as JSON or Python function")

    source = payload.get("patched_function_source")
    if not isinstance(source, str) or not source.strip():
        raise RetryPolicyError("LLM response missing 'patched_function_source'")

    return payload


def unwrap_for_source(func: Callable) -> Callable:
    try:
        return inspect.unwrap(func)
    except Exception:
        return func


def compile_replacement_function(
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
