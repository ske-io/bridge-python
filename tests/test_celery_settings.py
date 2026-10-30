import pytest
from celery import Celery

import ske_bridge.celery as ske_celery
from ske_bridge.celery import NoCeleryApp, configure, find_app, settings


def test_settings_point_celery_at_the_environments_sqs_queue(monkeypatch):
    monkeypatch.setenv("SKE_PROVIDER", "aws")
    monkeypatch.setenv("SQS_PREFIX", "https://sqs.ca-central-1.amazonaws.com/123456789012")
    monkeypatch.setenv("SQS_QUEUE", "default")
    monkeypatch.setenv("SQS_SUFFIX", "--shop-production")
    monkeypatch.setenv("AWS_REGION", "ca-central-1")

    assert settings() == {
        "broker_url": "sqs://",
        "broker_transport_options": {
            "region": "ca-central-1",
            "predefined_queues": {"default": {"url": "https://sqs.ca-central-1.amazonaws.com/123456789012/default--shop-production"}},
        },
        "task_default_queue": "default",
    }


@pytest.fixture
def aws(monkeypatch):
    monkeypatch.setenv("SKE_PROVIDER", "aws")
    monkeypatch.setenv("SQS_PREFIX", "https://sqs.us-east-1.amazonaws.com/123456789")
    monkeypatch.setenv("SQS_QUEUE", "default")
    monkeypatch.setenv("SQS_SUFFIX", "--ske-myapp-prod")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    return monkeypatch


def url(name):
    return {"url": f"https://sqs.us-east-1.amazonaws.com/123456789/{name}--ske-myapp-prod"}


def test_every_queue_the_environment_names_is_predefined(aws):
    aws.setenv("SQS_QUEUES", "default,emails,reports")

    options = settings()["broker_transport_options"]

    assert options["predefined_queues"] == {"default": url("default"), "emails": url("emails"), "reports": url("reports")}
    assert list(options["predefined_queues"]) == ["default", "emails", "reports"]
    assert settings()["task_default_queue"] == "default"


def test_the_default_queue_is_predefined_even_when_the_list_leaves_it_out(aws):
    aws.setenv("SQS_QUEUES", " emails , ,reports")

    assert list(settings()["broker_transport_options"]["predefined_queues"]) == ["default", "emails", "reports"]


@pytest.mark.parametrize("queues", [None, ""])
def test_without_a_queue_list_only_the_default_queue_is_predefined(aws, queues):
    if queues is None:
        aws.delenv("SQS_QUEUES", raising=False)
    else:
        aws.setenv("SQS_QUEUES", queues)

    assert settings()["broker_transport_options"]["predefined_queues"] == {"default": url("default")}


def test_a_task_routed_to_another_queue_reaches_its_sqs_queue(aws, sqs_sent):
    aws.setenv("SQS_QUEUES", "default,emails")
    app = Celery("routed", set_as_current=False)
    configure(app)

    @app.task(name="routed.send")
    def send():
        pass

    send.apply_async(queue="emails")
    send.apply_async()

    assert [m["QueueUrl"] for m in sqs_sent] == [url("emails")["url"], url("default")["url"]]


def test_configure_keeps_the_apps_own_queues_beside_the_environments(aws):
    aws.setenv("SQS_QUEUES", "default,emails")
    app = Celery("merged", set_as_current=False)
    app.conf.broker_transport_options = {"predefined_queues": {"legacy": {"url": "https://sqs.us-east-1.amazonaws.com/123456789/legacy"}}}

    configure(app)

    assert list(app.conf.broker_transport_options["predefined_queues"]) == ["legacy", "default", "emails"]


def test_settings_are_empty_off_the_platform():
    assert settings() == {}


@pytest.mark.parametrize("variable", ["SKE_CELERY_APP", "CELERY_APP"])
def test_find_app_by_name(monkeypatch, variable, celery_app):
    monkeypatch.setenv(variable, "tests.celery_app")

    assert find_app() is celery_app


def test_ske_celery_app_wins_over_the_fallback(monkeypatch, celery_app):
    monkeypatch.setenv("SKE_CELERY_APP", "tests.celery_app:app")

    assert find_app(fallback=Celery("other", set_as_current=False)) is celery_app


def test_an_explicit_app_wins_over_the_environment(monkeypatch, celery_app):
    monkeypatch.setenv("SKE_CELERY_APP", "tests.celery_app:app")
    explicit = Celery("explicit", set_as_current=False)

    assert find_app(explicit) is explicit


def test_the_fallback_when_nothing_names_an_app():
    other = Celery("other", set_as_current=False)

    assert find_app(fallback=other) is other


@pytest.fixture
def celery_state(monkeypatch):
    """Celery's record of apps and the current one, emptied for the test."""
    from celery import _state

    monkeypatch.setattr(_state._tls, "current_app", None)
    apps = set()
    monkeypatch.setattr(_state, "_get_active_apps", lambda: list(apps))
    return apps


def test_the_current_app(celery_state):
    from celery import _state

    current = Celery("current", set_as_current=False)
    _state._tls.current_app = current

    assert find_app() is current


def test_the_only_app_created_when_this_thread_has_no_current_one(celery_state):
    only = Celery("only", set_as_current=False)
    celery_state.add(only)

    assert find_app() is only


def test_no_app_is_an_error_not_celerys_default_app(celery_state):
    from celery import current_app

    celery_state.add(current_app._get_current_object())

    with pytest.raises(NoCeleryApp, match="SKE_CELERY_APP"):
        find_app()


def test_several_apps_need_naming(celery_state):
    celery_state.update({Celery("a", set_as_current=False), Celery("b", set_as_current=False)})

    with pytest.raises(NoCeleryApp, match="2 Celery apps"):
        find_app()


def test_configure_merges_into_the_apps_transport_options(monkeypatch):
    monkeypatch.setenv("SKE_PROVIDER", "aws")
    monkeypatch.setenv("SQS_PREFIX", "https://sqs.ca-central-1.amazonaws.com/123456789012")
    monkeypatch.setenv("SQS_SUFFIX", "--shop-production")
    monkeypatch.setenv("AWS_REGION", "ca-central-1")
    app = Celery("configured", set_as_current=False)
    app.conf.broker_transport_options = {
        "visibility_timeout": 900,
        "predefined_queues": {"reports": {"url": "https://sqs.ca-central-1.amazonaws.com/123456789012/reports"}},
    }

    configure(app)

    assert app.conf.broker_url == "sqs://"
    assert app.conf.task_default_queue == "default"
    assert app.conf.broker_transport_options == {
        "visibility_timeout": 900,
        "region": "ca-central-1",
        "predefined_queues": {
            "reports": {"url": "https://sqs.ca-central-1.amazonaws.com/123456789012/reports"},
            "default": {"url": "https://sqs.ca-central-1.amazonaws.com/123456789012/default--shop-production"},
        },
    }


def test_configure_does_nothing_off_aws():
    app = Celery("local", set_as_current=False)
    app.conf.broker_url = "redis://localhost"

    configure(app)

    assert app.conf.broker_url == "redis://localhost"


def test_find_app_imports_the_task_modules_once(monkeypatch):
    app = Celery("prepared", set_as_current=False)
    imported = []
    monkeypatch.setattr(app.loader, "import_default_modules", lambda: imported.append(True))
    monkeypatch.setattr(ske_celery, "_prepared", set())

    find_app(app)
    find_app(app)

    assert imported == [True]
