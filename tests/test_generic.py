import pytest

from ske_bridge.forwarded import Origin, origin
from ske_bridge.generic import AppImportError, NoQueueRunner, load, queue_runner


def test_load_follows_a_dotted_attribute():
    from tests import celery_app

    assert load("tests.celery_app:app.conf") is celery_app.app.conf


@pytest.mark.parametrize("target", ["main", "main:", ":app", ""])
def test_a_string_that_is_not_module_attribute_is_refused(target):
    with pytest.raises(AppImportError, match="is not an import string: name the app as module:attribute"):
        load(target)


def test_a_missing_module_names_the_import_string():
    with pytest.raises(AppImportError, match=r"cannot import the app 'nowhere\.main:app': ModuleNotFoundError: No module named 'nowhere'"):
        load("nowhere.main:app")


def test_a_missing_attribute_names_the_import_string():
    with pytest.raises(AppImportError, match=r"cannot import the app 'tests\.celery_app:missing': module 'tests\.celery_app' has no attribute 'missing'"):
        load("tests.celery_app:missing")


def test_the_import_error_is_an_import_error():
    assert issubclass(AppImportError, ImportError)


def test_without_a_named_celery_app_there_is_no_queue_runner():
    with pytest.raises(NoQueueRunner, match="set SKE_CELERY_APP"):
        queue_runner()


def test_the_named_celery_app_runs_the_queue(platform, celery_app):
    assert queue_runner().app is celery_app


@pytest.mark.parametrize(("headers", "scheme", "expected"), [
    (("198.51.100.7", "https", "app.example.com", "443"), "http", Origin("198.51.100.7", "https", "app.example.com", ("app.example.com", 443))),
    ((None, "http", "app.example.com", "80"), "https", Origin(None, "http", "app.example.com", ("app.example.com", 80))),
    ((None, "https", "app.example.com:8443", None), "http", Origin(None, "https", "app.example.com:8443", ("app.example.com", 8443))),
    ((None, "https", "app.example.com:8443", "443"), "http", Origin(None, "https", "app.example.com", ("app.example.com", 443))),
    ((None, "https", "[2001:db8::1]:8443", None), "http", Origin(None, "https", "[2001:db8::1]:8443", ("2001:db8::1", 8443))),
    ((None, "https", "[2001:db8::1]", None), "http", Origin(None, "https", "[2001:db8::1]", ("2001:db8::1", 443))),
    ((None, "https", None, "8443"), "http", Origin(None, "https")),
    ((None, None, "app.example.com", None), "https", Origin(None, None, "app.example.com", ("app.example.com", 443))),
    ((None, "gopher", None, "443"), "http", Origin()),
    (("a, 198.51.100.7", "http, https", "evil.example, app.example.com", "nope"), "http", Origin("198.51.100.7", "https", "app.example.com", ("app.example.com", 443))),
    ((None, None, None, None), "http", Origin()),
])
def test_origin_reads_the_last_entry_of_each_header(headers, scheme, expected):
    assert origin(*headers, scheme) == expected
