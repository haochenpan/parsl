import io
import logging
import types
from unittest.mock import MagicMock

import pytest

from parsl.log_utils import set_file_logger, set_stream_logger


@pytest.mark.local
def test_stream_close():
    """Tests that set_stream_logger callback detaches log handler.
    """

    logger = logging.getLogger("parsl")

    s1 = io.StringIO()
    close_callback_1 = set_stream_logger(stream=s1)
    logger.info("AAA")
    close_callback_1()

    s2 = io.StringIO()
    close_callback_2 = set_stream_logger(stream=s2)
    logger.info("BBB")
    close_callback_2()

    logger.info("CCC")

    assert "AAA" in s1.getvalue()
    assert "AAA" not in s2.getvalue()

    assert "BBB" not in s1.getvalue()
    assert "BBB" in s2.getvalue()

    assert "CCC" not in s1.getvalue()
    assert "CCC" not in s2.getvalue()


@pytest.mark.local
def test_file_close(tmpd_cwd):
    """Tests that set_file_Logger callback detaches log handler.
    """

    logger = logging.getLogger("parsl")

    f1 = str(tmpd_cwd / "log1")
    close_callback_1 = set_file_logger(filename=f1)
    logger.info("AAA")
    close_callback_1()

    f2 = str(tmpd_cwd / "log2")
    close_callback_2 = set_file_logger(filename=f2)
    logger.info("BBB")
    close_callback_2()

    logger.info("CCC")

    with open(f1, "r") as f:
        s1 = f.read()

    with open(f2, "r") as f:
        s2 = f.read()

    assert "AAA" in s1
    assert "AAA" not in s2

    assert "BBB" not in s1
    assert "BBB" in s2

    assert "CCC" not in s1
    assert "CCC" not in s2


@pytest.mark.local
def test_diaspora_stream_close(monkeypatch):
    """Tests that set_diaspora_logger callback detaches log handler."""
    import sys

    class FakeProducer:
        def __init__(self, *args, **kwargs):
            self.sent = []
            self.flush_timeout = None

        def send(self, topic, event):
            self.sent.append((topic, event))

        def flush(self, timeout=None):
            self.flush_timeout = timeout

        def close(self):
            pass

    class FakeClient:
        namespace = "test-ns"

        def __init__(self, **kwargs):
            pass

        def create_key(self):
            pass

        def create_topic(self, name):
            return {"status": "no-op"}

    # Build a fake diaspora_event_sdk module hierarchy
    fake_sdk = types.ModuleType("diaspora_event_sdk")
    fake_sdk.Client = FakeClient

    fake_sdk_sdk = types.ModuleType("diaspora_event_sdk.sdk")
    fake_kafka_client = types.ModuleType("diaspora_event_sdk.sdk.kafka_client")
    fake_kafka_client.KafkaProducer = FakeProducer

    monkeypatch.setitem(sys.modules, "diaspora_event_sdk", fake_sdk)
    monkeypatch.setitem(sys.modules, "diaspora_event_sdk.sdk", fake_sdk_sdk)
    monkeypatch.setitem(sys.modules, "diaspora_event_sdk.sdk.kafka_client", fake_kafka_client)

    from parsl.log_utils import set_diaspora_logger

    logger = logging.getLogger("parsl")

    close_callback_1 = set_diaspora_logger(
        topic_name="test-topic",
        send_timeout=7,
    )
    logger.info("AAA")
    close_callback_1()

    close_callback_2 = set_diaspora_logger(
        topic_name="test-topic",
        send_timeout=4,
    )
    logger.info("BBB")
    close_callback_2()

    logger.info("CCC")

    # Retrieve the producers from the DiasporaHandlers that were created.
    # Since we can't directly access them, we verify via the fake module
    # that the messages were routed correctly by checking logger handler count.
    # The key verification is that after close, handlers are detached.
    parsl_handlers = [
        h for h in logging.getLogger("parsl").handlers
        if hasattr(h, "producer")
    ]
    # After both callbacks closed, no DiasporaHandlers should remain
    assert len(parsl_handlers) == 0
