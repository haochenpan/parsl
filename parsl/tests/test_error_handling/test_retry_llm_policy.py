import pytest

from parsl.retries.policy import build_retry_llm_policy
from parsl.retries.types import RetryDirective


class DummyConfig:
    retries = 1


class DummyDFK:
    run_id = "run-xyz"
    config = DummyConfig()


def demo_func():
    return "original"


@pytest.mark.local
def test_retry_llm_policy_non_allowlisted_exception(monkeypatch):
    class FakeClient:
        def __init__(self):
            self.calls = 0

        def generate_patch(self, **_kwargs):
            self.calls += 1
            return "{}"

    client = FakeClient()

    policy = build_retry_llm_policy(
        llm_client=client,
        diaspora_topic="topic-parsl-local",
    )

    task_record = {
        "dfk": DummyDFK(),
        "id": 1,
        "try_id": 0,
        "func": demo_func,
        "func_name": "demo_func",
        "retry_patch_history": [],
    }

    decision = policy(ValueError("bad"), task_record)
    assert decision == 1.0
    assert client.calls == 0


@pytest.mark.local
def test_retry_llm_policy_allowlisted_exception_with_patch(monkeypatch):
    class FakeClient:
        def generate_patch(self, **_kwargs):
            return (
                '{"patched_function_source": '
                '"def demo_func():\\n    return \\\"patched\\\"", '
                '"summary": "fixed"}'
            )

    def fake_context(**_kwargs):
        return {
            "kafka_topic": "ns.topic-parsl-local",
            "matched_count": 2,
            "events": [{"message": "syntax error"}],
        }

    monkeypatch.setattr("parsl.retries.policy.fetch_diaspora_context", fake_context)

    policy = build_retry_llm_policy(
        llm_client=FakeClient(),
        diaspora_topic="topic-parsl-local",
    )

    task_record = {
        "dfk": DummyDFK(),
        "id": 2,
        "try_id": 0,
        "func": demo_func,
        "func_name": "demo_func",
        "retry_patch_history": [],
    }

    decision = policy(SyntaxError("invalid syntax"), task_record)
    assert isinstance(decision, RetryDirective)
    assert decision.patch is not None
    assert callable(decision.patch.func)


@pytest.mark.local
def test_retry_llm_policy_parses_fenced_python_response(monkeypatch):
    class FakeClient:
        def generate_patch(self, **_kwargs):
            return (
                "<think>internal reasoning</think>\n"
                "```python\n"
                "def demo_func():\n"
                "    return \"patched-from-code\"\n"
                "```\n"
            )

    def fake_context(**_kwargs):
        return {"matched_count": 0, "events": []}

    monkeypatch.setattr("parsl.retries.policy.fetch_diaspora_context", fake_context)

    policy = build_retry_llm_policy(
        llm_client=FakeClient(),
        diaspora_topic="topic-parsl-local",
    )

    task_record = {
        "dfk": DummyDFK(),
        "id": 12,
        "try_id": 0,
        "func": demo_func,
        "func_name": "demo_func",
        "retry_patch_history": [],
    }

    decision = policy(SyntaxError("invalid syntax"), task_record)
    assert isinstance(decision, RetryDirective)
    assert decision.patch is not None
    assert callable(decision.patch.func)


@pytest.mark.local
def test_retry_llm_policy_diaspora_failure_aborts(monkeypatch):
    class FakeClient:
        def __init__(self):
            self.calls = 0

        def generate_patch(self, **_kwargs):
            self.calls += 1
            return "{}"

    def raise_context(**_kwargs):
        raise RuntimeError("context unavailable")

    monkeypatch.setattr("parsl.retries.policy.fetch_diaspora_context", raise_context)

    client = FakeClient()
    policy = build_retry_llm_policy(
        llm_client=client,
        diaspora_topic="topic-parsl-local",
    )

    task_record = {
        "dfk": DummyDFK(),
        "id": 3,
        "try_id": 0,
        "func": demo_func,
        "func_name": "demo_func",
        "retry_patch_history": [],
    }

    decision = policy(SyntaxError("invalid syntax"), task_record)
    assert decision == 2.0
    assert client.calls == 0
