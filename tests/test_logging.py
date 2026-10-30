import io
import json
import logging
import sys

import pytest

from ske_bridge import logging as ske_logging
from ske_bridge.logging import JsonFormatter, configure, request_id


class Tree:
    """A logger tree of its own, so the process's is left as the test runner
    set it up."""

    def __init__(self):
        self.manager = logging.Manager(logging.RootLogger(logging.WARNING))
        self.manager.setLoggerClass(logging.Logger)
        self.root = self.manager.root

    def get(self, name):
        return self.manager.getLogger(name)

    def configure(self):
        configure(self.manager)


@pytest.fixture
def tree():
    return Tree()


def record_line(logger_name="app", **kwargs):
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter())
    logger = logging.Logger(logger_name)
    logger.addHandler(handler)
    return logger, stream


def test_a_record_is_one_json_line_the_logs_page_reads():
    logger, stream = record_line("orders")

    logger.warning("order %s is late", 7, extra={"order": 7})

    line = stream.getvalue()
    assert line.count("\n") == 1
    record = json.loads(line)
    assert record["message"] == "order 7 is late"
    assert record["level_name"] == "WARNING"
    assert record["channel"] == "orders"
    assert record["context"] == {"order": 7}
    assert record["datetime"].endswith("+00:00")
    assert record["extra"] == {}


def test_the_exception_is_where_the_logs_page_reads_it():
    logger, stream = record_line()

    def inner():
        raise ValueError("bad order")

    try:
        try:
            inner()
        except ValueError as e:
            raise RuntimeError("checkout failed") from e
    except RuntimeError:
        logger.exception("checkout")

    exception = json.loads(stream.getvalue())["context"]["exception"]
    assert exception["class"] == "RuntimeError"
    assert exception["message"] == "checkout failed"
    assert exception["file"].startswith(__file__ + ":")
    assert exception["trace"] == []
    previous = exception["previous"]
    assert previous["class"] == "ValueError"
    # Raised in inner(), called from the test: most recent call first.
    assert previous["file"].startswith(__file__ + ":")
    assert len(previous["trace"]) == 1 and previous["trace"][0].startswith(__file__ + ":")


def test_a_qualified_class_name_outside_builtins():
    from json import JSONDecodeError

    logger, stream = record_line()
    try:
        json.loads("{")
    except JSONDecodeError:
        logger.exception("parse")

    assert json.loads(stream.getvalue())["context"]["exception"]["class"] == "json.decoder.JSONDecodeError"


def test_the_request_id_is_in_the_context():
    logger, stream = record_line()
    token = request_id.set("req-123")
    try:
        logger.info("served")
    finally:
        request_id.reset(token)

    assert json.loads(stream.getvalue())["context"]["request_id"] == "req-123"


def test_the_root_logger_writes_json_to_stderr(platform, tree):
    tree.configure()

    [handler] = tree.root.handlers
    assert handler.stream is sys.stderr
    assert isinstance(handler.formatter, JsonFormatter)


def test_a_logger_that_reaches_root_loses_its_terminal_handler_so_nothing_is_written_twice(platform, tree):
    django = tree.get("django")
    django.addHandler(logging.StreamHandler(sys.stderr))

    tree.configure()

    assert django.handlers == []
    assert len(tree.root.handlers) == 1


def test_a_logger_that_does_not_propagate_keeps_its_handler_in_the_format(platform, tree):
    gunicorn = tree.get("gunicorn.error")
    gunicorn.propagate = False
    handler = logging.StreamHandler(sys.stderr)
    gunicorn.addHandler(handler)

    tree.configure()

    assert gunicorn.handlers == [handler]
    assert isinstance(handler.formatter, JsonFormatter)


def test_file_handlers_are_left_alone(platform, tree, tmp_path):
    handler = logging.FileHandler(tmp_path / "app.log")
    tree.get("app").addHandler(handler)

    tree.configure()

    assert handler.formatter is None
    handler.close()


def test_an_existing_root_handler_is_given_the_format_not_doubled(platform, tree):
    handler = logging.StreamHandler(sys.stderr)
    tree.root.addHandler(handler)

    tree.configure()
    tree.configure()

    assert tree.root.handlers == [handler]
    assert isinstance(handler.formatter, JsonFormatter)


def test_plain_keeps_the_apps_logging(platform, tree, monkeypatch):
    monkeypatch.setenv("SKE_LOG_FORMAT", "plain")

    tree.configure()

    assert tree.root.handlers == []


def test_off_the_platform_logging_is_the_apps(tree):
    tree.configure()

    assert tree.root.handlers == []
    assert not ske_logging.enabled()
