import json
import logging
import os
from types import SimpleNamespace

import django
import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import Client

from ske_bridge.django.apps import MIDDLEWARE, SMTP_BACKEND, install_middleware, trust_forwarded_proto, use_mail_service
from tests.conftest import record


@pytest.fixture(scope="module", autouse=True)
def django_project():
    os.environ["DJANGO_SETTINGS_MODULE"] = "tests.django_project.settings"
    os.environ["SKE_PROJECT_TYPE"] = "django"
    try:
        django.setup()
    finally:
        del os.environ["SKE_PROJECT_TYPE"]


@pytest.fixture
def client(platform):
    return Client(enforce_csrf_checks=True)


def queue(client, records, internal=True, **options):
    headers = {"X-Ske-Internal": "true"} if internal else {}
    payload = {"source": "sqs", "records": records}
    if options:
        payload["options"] = options
    return client.post("/__ske/queue", json.dumps(payload), content_type="application/json", headers=headers)


def test_the_app_puts_its_middleware_first_on_the_platform():
    from django.conf import settings

    assert settings.MIDDLEWARE[0] == MIDDLEWARE
    assert settings.MIDDLEWARE.count(MIDDLEWARE) == 1


def test_install_middleware_keeps_a_listed_position():
    settings = SimpleNamespace(MIDDLEWARE=("a", MIDDLEWARE))

    install_middleware(settings)

    assert list(settings.MIDDLEWARE) == ["a", MIDDLEWARE]


def test_the_app_leaves_middleware_alone_off_the_platform(monkeypatch):
    from django.apps import apps
    from django.conf import settings

    before = list(settings.MIDDLEWARE)
    monkeypatch.setattr(settings, "MIDDLEWARE", [m for m in before if m != MIDDLEWARE])

    apps.get_app_config("ske_bridge").ready()

    assert MIDDLEWARE not in settings.MIDDLEWARE


def test_the_queue_runs_a_batch_past_host_csrf_and_upload_limits(client, publish):
    from tests.celery_app import calls

    records = [record(publish("tests.add", (i, i)), message_id=f"m{i}") for i in range(3)]

    response = queue(client, records)

    assert response.status_code == 200
    body = response.json()
    assert body["processed"] == 3
    assert [o["deleted"] for o in body["outcomes"]] == [True, True, True]
    assert calls == [("add", 0, 0), ("add", 1, 1), ("add", 2, 2)]


def test_the_queue_reports_a_partial_batch(client, publish):
    response = queue(client, [record(publish("tests.add", (1, 1)), message_id="ok"), record(publish("tests.fail"), message_id="bad")])

    outcomes = {o["message_id"]: o for o in response.json()["outcomes"]}
    assert outcomes["ok"]["deleted"]
    assert outcomes["bad"]["failed"] and outcomes["bad"]["dead_letter"]


def test_the_queue_needs_the_internal_header(client, publish):
    response = queue(client, [record(publish("tests.add", (1, 1)))], internal=False)

    assert response.status_code == 404
    assert response.json() == {"error": "not found"}


def test_csrf_still_guards_the_applications_own_posts(client):
    assert client.post("/", {}, headers={"host": "app.example.com"}).status_code == 403


def test_a_missing_celery_app_answers_500(client, publish, monkeypatch):
    import ske_bridge.django.middleware as middleware
    from ske_bridge.routes import Routes

    def no_app():
        from ske_bridge.celery import NoCeleryApp

        raise NoCeleryApp("no Celery app is configured")

    monkeypatch.setattr(middleware, "routes", Routes(no_app))
    response = queue(client, [record(publish("tests.add", (1, 1)))])

    assert response.status_code == 500
    assert "no Celery app" in response.json()["error"]


def test_health_answers_for_readiness(client):
    response = client.get("/__ske/health")

    assert (response.status_code, response.json()) == (200, {"status": "ok"})


def test_other_paths_reach_the_application(client):
    # The application's own host check still applies to its own routes.
    assert client.get("/").status_code == 400
    assert client.get("/", headers={"host": "app.example.com"}).content == b"home"


def test_ske_schedule_sends_the_due_entries(platform, sqs_sent, monkeypatch, capsys):
    import ske_bridge.celery.scheduler as scheduler
    from tests.test_scheduler import MONDAY_0930_PARIS

    monkeypatch.setattr(scheduler, "current_minute", lambda: MONDAY_0930_PARIS)

    call_command("ske_schedule")

    out = capsys.readouterr().out
    assert "sent every-minute (tests.record)" in out
    assert "sent weekday-mornings (tests.record)" in out
    assert len(sqs_sent) == 3


def test_ske_schedule_fails_when_an_entry_cannot_be_sent(platform, celery_app, monkeypatch):
    monkeypatch.setattr(celery_app.conf, "beat_schedule", {"bad": {"task": "tests.record", "schedule": 60, "options": {"queue": "nowhere"}}})

    with pytest.raises(CommandError, match="1 scheduled task"):
        call_command("ske_schedule")


FORWARDED = {
    "host": "app.example.com",
    "x-forwarded-for": "203.0.113.9, 198.51.100.7",
    "x-forwarded-proto": "https",
    "x-forwarded-host": "app.example.com",
    "x-forwarded-port": "443",
}


def test_the_client_is_the_address_the_platform_appended(client):
    seen = client.get("/client", headers=FORWARDED, REMOTE_ADDR="127.0.0.1").json()

    assert seen == {"remote_addr": "198.51.100.7", "secure": True, "url": "https://app.example.com/"}


def test_a_forwarded_for_entry_that_is_not_an_address_leaves_the_connections(client):
    seen = client.get("/client", headers={**FORWARDED, "x-forwarded-for": "198.51.100.7, unknown"}, REMOTE_ADDR="127.0.0.1").json()

    assert seen["remote_addr"] == "127.0.0.1"


def test_without_forwarded_headers_nothing_changes(client):
    seen = client.get("/client", headers={"host": "app.example.com"}, REMOTE_ADDR="127.0.0.1").json()

    assert seen == {"remote_addr": "127.0.0.1", "secure": False, "url": "http://app.example.com/"}


def test_off_the_platform_the_middleware_trusts_nothing():
    seen = Client().get("/client", headers={**FORWARDED, "x-forwarded-proto": "http"}, REMOTE_ADDR="127.0.0.1").json()

    assert seen["remote_addr"] == "127.0.0.1"


def test_the_app_trusts_the_forwarded_proto_on_the_platform():
    from django.conf import settings

    assert settings.SECURE_PROXY_SSL_HEADER == ("HTTP_X_FORWARDED_PROTO", "https")


def test_a_projects_own_proxy_ssl_header_is_kept():
    settings = SimpleNamespace(SECURE_PROXY_SSL_HEADER=("HTTP_X_SCHEME", "https"))

    trust_forwarded_proto(settings)

    assert settings.SECURE_PROXY_SSL_HEADER == ("HTTP_X_SCHEME", "https")


def project_mail_settings(**overrides):
    return SimpleNamespace(**{
        "EMAIL_BACKEND": SMTP_BACKEND,
        "EMAIL_HOST": "smtp.example.com",
        "EMAIL_PORT": 587,
        "EMAIL_USE_TLS": True,
        "EMAIL_USE_SSL": False,
        "EMAIL_HOST_USER": "app",
        "EMAIL_HOST_PASSWORD": "secret",
        **overrides,
    })


@pytest.fixture
def mail_service(platform, monkeypatch):
    monkeypatch.setenv("MAIL_HOST", "127.0.0.1")
    monkeypatch.setenv("MAIL_PORT", "2525")


def test_smtp_mail_goes_to_the_platforms_mail_service(mail_service):
    settings = project_mail_settings()

    use_mail_service(settings)

    assert vars(settings) == {
        "EMAIL_BACKEND": SMTP_BACKEND,
        "EMAIL_HOST": "127.0.0.1",
        "EMAIL_PORT": 2525,
        "EMAIL_USE_TLS": False,
        "EMAIL_USE_SSL": False,
        "EMAIL_HOST_USER": "",
        "EMAIL_HOST_PASSWORD": "",
    }


def test_the_default_backend_is_smtp(mail_service):
    settings = project_mail_settings()
    del settings.EMAIL_BACKEND

    use_mail_service(settings)

    assert settings.EMAIL_HOST == "127.0.0.1"


@pytest.mark.parametrize("backend", [
    "django.core.mail.backends.console.EmailBackend",
    "django.core.mail.backends.locmem.EmailBackend",
    "anymail.backends.amazon_ses.EmailBackend",
])
def test_other_backends_are_left_alone(mail_service, backend):
    settings = project_mail_settings(EMAIL_BACKEND=backend)

    use_mail_service(settings)

    assert settings.EMAIL_HOST == "smtp.example.com" and settings.EMAIL_HOST_USER == "app"


@pytest.mark.parametrize("env", [{}, {"MAIL_HOST": "127.0.0.1"}, {"MAIL_PORT": "2525"}, {"MAIL_HOST": "127.0.0.1", "MAIL_PORT": "smtp"}])
def test_without_a_usable_mail_service_the_settings_are_the_projects(platform, monkeypatch, env):
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    settings = project_mail_settings()

    use_mail_service(settings)

    assert settings.EMAIL_HOST == "smtp.example.com"


def test_off_the_platform_the_settings_are_the_projects(monkeypatch):
    monkeypatch.setenv("MAIL_HOST", "127.0.0.1")
    monkeypatch.setenv("MAIL_PORT", "2525")
    settings = project_mail_settings()

    use_mail_service(settings)

    assert settings.EMAIL_HOST == "smtp.example.com"


def test_djangos_smtp_backend_delivers_to_the_service_without_tls_or_login(mail_service, smtp_catcher, monkeypatch):
    from django.conf import settings
    from django.core.mail import send_mail
    from django.test import override_settings

    monkeypatch.setenv("MAIL_PORT", str(smtp_catcher.port))
    configured = project_mail_settings()
    use_mail_service(configured)

    with override_settings(**vars(configured)):
        send_mail("hi", "hello", "app@example.com", ["user@example.com"])

    assert b"Subject: hi" in smtp_catcher.messages[0]
    assert settings.EMAIL_BACKEND == SMTP_BACKEND


@pytest.fixture
def down(platform, monkeypatch):
    monkeypatch.setenv("MAINTENANCE_MODE", "true")
    monkeypatch.setenv("MAINTENANCE_MODE_SECRET", "preview-secret")


SITE = {"host": "app.example.com", "x-forwarded-proto": "https"}


def test_maintenance_answers_503_with_the_projects_template(down, client):
    response = client.get("/orders", headers=SITE)

    assert response.status_code == 503
    assert response.content.decode().strip() == "<h1>BACK SOON</h1>"


def test_maintenance_falls_back_to_a_minimal_page(down, client):
    from django.test import override_settings

    from ske_bridge.maintenance import DEFAULT_PAGE

    with override_settings(TEMPLATES=[]):
        response = client.get("/", headers=SITE)

    assert response.status_code == 503
    assert response.content == DEFAULT_PAGE


def test_maintenance_keeps_readiness_and_the_queue(down, client, publish):
    assert client.get("/__ske/health").status_code == 200
    assert queue(client, [record(publish("tests.add", (1, 1)))]).json()["outcomes"][0]["deleted"]


def test_the_platforms_warmup_request_passes_maintenance(down, client):
    assert client.get("/", headers={**SITE, "X-Ske-Internal": "true"}).status_code == 200


def test_the_secret_sets_a_cookie_that_lets_the_visitor_in(down, client):
    bypass = client.get("/preview-secret", headers=SITE)

    assert bypass.status_code == 302 and bypass["Location"] == "/"
    assert "Secure" in bypass["Set-Cookie"] and "HttpOnly" in bypass["Set-Cookie"]
    cookie = bypass["Set-Cookie"].split(";", 1)[0]
    assert client.get("/", headers={**SITE, "cookie": cookie}).content == b"home"


def test_without_maintenance_the_secret_is_an_ordinary_path(platform, client):
    assert client.get("/preview-secret", headers=SITE).status_code == 404


def test_the_request_id_reaches_the_logs(client):
    assert client.get("/request-id", headers={**SITE, "x-request-id": "req-9"}).content == b"req-9"


def test_the_app_writes_its_logs_in_the_platforms_format():
    from ske_bridge.logging import JsonFormatter

    root = logging.getLogger()
    assert any(isinstance(h.formatter, JsonFormatter) for h in root.handlers)
    # Django's own console handler would write each record a second time.
    assert not any(type(h) is logging.StreamHandler for h in logging.getLogger("django").handlers)
