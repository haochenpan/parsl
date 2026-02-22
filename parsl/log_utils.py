"""This module contains helpers for configuring logging. By default,
`set_file_logger` is invoked by the DataFlowKernel initializer to log
parsl messages to parsl.log.

`set_stream_logger` which by default logs to stderr, can be useful
when working in a Jupyter notebook.
"""
import io
import logging
from typing import Any, Callable, Optional

import typeguard

DEFAULT_FORMAT = (
    "%(created)f %(asctime)s %(processName)s-%(process)d "
    "%(threadName)s-%(thread)d %(name)s:%(lineno)d %(funcName)s %(levelname)s: "
    "%(message)s"
)


@typeguard.typechecked
def set_stream_logger(name: str = 'parsl',
                      level: int = logging.DEBUG,
                      format_string: Optional[str] = None,
                      stream: Optional[io.TextIOBase] = None) -> Callable[[], None]:
    """Add a stream log handler.

    Args:
         - name (string) : Set the logger name.
         - level (logging.LEVEL) : Set to logging.DEBUG by default.
         - format_string (string) : Set to None by default.
         - stream (io.TextIOWrapper) : Specify sys.stdout or sys.stderr for stream.
            If not specified, the default stream for logging.StreamHandler is used.
    """
    if format_string is None:
        # format_string = "%(asctime)s %(name)s [%(levelname)s] Thread:%(thread)d %(message)s"
        format_string = "%(asctime)s %(name)s:%(lineno)d [%(levelname)s]  %(message)s"

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(stream)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Concurrent.futures errors are also of interest, as exceptions
    # which propagate out of the top of a callback are logged this way
    # and then discarded. (see #240)
    futures_logger = logging.getLogger("concurrent.futures")
    futures_logger.addHandler(handler)

    def unregister_callback():
        logger.removeHandler(handler)
        futures_logger.removeHandler(handler)

    return unregister_callback


@typeguard.typechecked
def set_file_logger(filename: str,
                    name: str = 'parsl',
                    level: int = logging.DEBUG,
                    format_string: Optional[str] = None) -> Callable[[], None]:
    """Add a file log handler.

    Args:
        - filename (string): Name of the file to write logs to
        - name (string): Logger name
        - level (logging.LEVEL): Set the logging level.
        - format_string (string): Set the format string

    Returns:
        - a callable which, when invoked, will reverse the log handler
          attachments made by this call. (compare to how object based pieces
          of parsl model this as a close/shutdown/cleanup method on the
          object))
    """
    if format_string is None:
        format_string = DEFAULT_FORMAT

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(filename)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # see note in set_stream_logger for notes about logging
    # concurrent.futures
    futures_logger = logging.getLogger("concurrent.futures")
    futures_logger.addHandler(handler)

    def unregister_callback():
        logger.removeHandler(handler)
        futures_logger.removeHandler(handler)

    return unregister_callback


class DiasporaHandler(logging.Handler):
    """Logging handler that emits records into a Diaspora-backed Kafka topic."""

    def __init__(self,
                 producer: Any,
                 kafka_topic: str,
                 send_timeout: int) -> None:
        super().__init__()
        self.producer = producer
        self.kafka_topic = kafka_topic
        self.send_timeout = send_timeout

    @staticmethod
    def _json_safe(value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value

        if isinstance(value, bytes):
            return value.decode(errors="replace")
        if isinstance(value, dict):
            return {str(k): DiasporaHandler._json_safe(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [DiasporaHandler._json_safe(v) for v in value]
        return repr(value)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            # Serialize the full LogRecord payload (including any extra fields)
            # so downstream consumers can choose their own projection.
            event = {k: self._json_safe(v) for k, v in record.__dict__.items()}
            event["message"] = record.getMessage()
            if self.formatter is not None:
                event["formatted"] = self.format(record)
            self.producer.send(self.kafka_topic, event)
        except Exception:
            self.handleError(record)

    def close(self) -> None:
        if hasattr(self.producer, "flush"):
            try:
                self.producer.flush(timeout=self.send_timeout)
            except TypeError:
                self.producer.flush()
            except Exception:
                # Logging close should be best-effort and not break caller shutdown.
                pass

        super().close()


@typeguard.typechecked
def set_diaspora_logger(topic_name: str = "topic-parsl-logs",
                        name: str = 'parsl',
                        level: int = logging.INFO,
                        format_string: Optional[str] = None,
                        send_timeout: int = 30) -> Callable[[], None]:
    """Add a Diaspora event-fabric logger.

    Args:
        - topic_name (string): Kafka topic name. Can be either short topic name
          (without namespace) or a fully-qualified ``namespace.topic``.
        - name (string): Logger name to attach the handler to.
        - level (logging.LEVEL): Set the logging level. Defaults to INFO.
        - format_string (string): Optional custom formatted output added as
          event field ``formatted``.
        - send_timeout (int): Ack timeout in seconds while waiting for queued sends on close.

    Returns:
        - a callable which, when invoked, will reverse the log handler
          attachments made by this call.
    """
    try:
        from diaspora_event_sdk import Client as GlobusClient
        from diaspora_event_sdk.sdk.kafka_client import KafkaProducer
    except ImportError as e:
        raise RuntimeError(
            "diaspora-event-sdk with kafka-python support is required. "
            "Install with: pip install -e '.[diaspora]'. "
            "Then run: python examples/diaspora/diaspora.py setup"
        ) from e

    try:
        client = GlobusClient()
        client.create_key()
        create_topic_name = topic_name.split(".", 1)[1] if "." in topic_name else topic_name
        topic_result = client.create_topic(create_topic_name)
        if isinstance(topic_result, dict):
            status = topic_result.get("status")
            if status not in {"success", "no-op", None}:
                raise RuntimeError(f"create_topic failed with status={status!r}")

        kafka_topic = topic_name if "." in topic_name else f"{client.namespace}.{topic_name}"
        producer = KafkaProducer(kafka_topic)
    except Exception as e:
        raise RuntimeError(
            "Failed to initialize Diaspora logging producer. "
            "Run: python examples/diaspora/diaspora.py setup"
        ) from e

    if format_string is None:
        format_string = DEFAULT_FORMAT

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = DiasporaHandler(producer, kafka_topic, send_timeout)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    futures_logger = logging.getLogger("concurrent.futures")
    futures_logger.addHandler(handler)

    def unregister_callback():
        logger.removeHandler(handler)
        futures_logger.removeHandler(handler)
        handler.close()
        if hasattr(producer, "close"):
            producer.close()

    return unregister_callback
