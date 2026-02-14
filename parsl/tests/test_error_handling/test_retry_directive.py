import pytest

import parsl
from parsl import python_app
from parsl.app.errors import wrap_error
from parsl.retries.types import RetryDirective, RetryPatch
from parsl.tests.configs.local_threads import fresh_config


@python_app
def fail_once(marker_file: str):
    import os

    if not os.path.exists(marker_file):
        with open(marker_file, "w", encoding="utf-8") as handle:
            handle.write("first\n")
        raise RuntimeError("first failure")
    return "ok"


@python_app
def always_fails():
    raise NameError("always bad")


@pytest.mark.local
def test_retry_handler_float_compatibility(tmpd_cwd):
    def float_retry_handler(_exc, _task_record):
        return 1.0

    config = fresh_config()
    config.retries = 1
    config.retry_handler = float_retry_handler

    parsl.load(config)
    try:
        future = fail_once(str(tmpd_cwd / "marker.txt"))
        assert future.result() == "ok"
    finally:
        parsl.clear()


@pytest.mark.local
def test_retry_directive_applies_patch():
    def patch_retry_handler(_exc, _task_record):
        def patched():
            return "patched"

        return RetryDirective(
            cost=1.0,
            patch=RetryPatch(func=wrap_error(patched), patch_id="unit-patch"),
            reason="unit-test",
        )

    config = fresh_config()
    config.retries = 1
    config.retry_handler = patch_retry_handler

    parsl.load(config)
    try:
        future = always_fails()
        assert future.result() == "patched"
        assert future.task_record["retry_patch_applied"] is True
        assert len(future.task_record["retry_patch_history"]) == 1
    finally:
        parsl.clear()


@pytest.mark.local
def test_retry_directive_rejects_same_callable():
    def same_callable_handler(_exc, task_record):
        return RetryDirective(
            cost=1.0,
            patch=RetryPatch(func=task_record["func"], patch_id="same-callable"),
            reason="unit-test",
        )

    config = fresh_config()
    config.retries = 1
    config.retry_handler = same_callable_handler

    parsl.load(config)
    try:
        future = always_fails()
        with pytest.raises(ValueError, match="distinct callable"):
            future.result()
        assert "distinct callable" in str(future.task_record["retry_patch_last_error"])
    finally:
        parsl.clear()


@pytest.mark.local
def test_retry_directive_rejects_unserializable_callable():
    class BadCallable:
        def __call__(self):
            return "never"

        def __getstate__(self):
            raise TypeError("cannot serialize")

    def bad_serialization_handler(_exc, _task_record):
        return RetryDirective(
            cost=1.0,
            patch=RetryPatch(func=BadCallable(), patch_id="bad-serialization"),
            reason="unit-test",
        )

    config = fresh_config()
    config.retries = 1
    config.retry_handler = bad_serialization_handler

    parsl.load(config)
    try:
        future = always_fails()
        with pytest.raises(RuntimeError, match="serialization preflight"):
            future.result()
        assert "serialization preflight" in str(future.task_record["retry_patch_last_error"])
    finally:
        parsl.clear()
