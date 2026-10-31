import json
from pathlib import Path

import pytest
from celery import Celery
from flask import Blueprint, Flask, current_app

from ske_bridge.flask import SkeBridge
from tests.conftest import record


STATIC = Path(__file__).parent / "fixtures" / "static"


def make_app(**kwargs):
    app = Flask("tests", static_folder=str(STATIC))
    app.register_blueprint(Blueprint("admin", "tests", static_folder=str(STATIC), static_url_path="/static", url_prefix="/admin"))

    @app.before_request
    def login_required():
        # An application-wide hook the platform's requests must not hit.
        if not current_app.config.get("open"):
            return "login required", 401

    @app.route("/")
    def home():
        return "home"

    @app.route("/client")
    def seen():
        from flask import request, url_for

        return {"remote_addr": request.remote_addr, "scheme": request.scheme, "host": request.host, "url": url_for("home", _external=True)}

    SkeBridge(app, **kwargs)
    return app


def queue(client, records, internal=True):
    headers = {"X-Ske-Internal": "true"} if internal else {}
    return client.post("/__ske/queue", data=json.dumps({"source": "sqs", "records": records}), content_type="application/json", headers=headers)


@pytest.fixture
def app(platform):
    return make_app()


def test_the_queue_runs_tasks_before_the_applications_hooks(app, publish):
    from tests.celery_app import calls

    response = queue(app.test_client(), [record(publish("tests.add", (2, 2)))])

    assert response.status_code == 200
    assert response.json["processed"] == 1
    assert response.json["outcomes"][0]["deleted"]
    assert calls == [("add", 2, 2)]


def test_tasks_run_inside_an_app_context(platform, celery_app, publish, monkeypatch):
    seen = []

    @celery_app.task(name="tests.context")
    def context():
        seen.append(current_app.name)

    app = make_app()
    queue(app.test_client(), [record(publish("tests.context"))])

    assert seen == ["tests"]


def test_the_queue_reports_retries_and_failures(app, publish):
    response = queue(app.test_client(), [record(publish("tests.retry"), message_id="r"), record(publish("tests.fail"), message_id="f")])

    retried, failed = response.json["outcomes"]
    assert (retried["message_id"], retried["deleted"], retried["error"][:8]) == ("r", True, "retried:")
    assert (failed["message_id"], failed["failed"], failed["dead_letter"]) == ("f", True, True)


def test_the_queue_needs_the_internal_header(app, publish):
    response = queue(app.test_client(), [record(publish("tests.add", (1, 1)))], internal=False)

    assert response.status_code == 404
    assert response.json == {"error": "not found"}


def test_health_answers_for_readiness(app):
    response = app.test_client().get("/__ske/health")

    assert (response.status_code, response.json) == (200, {"status": "ok"})


def test_other_paths_reach_the_application(app):
    assert app.test_client().get("/").status_code == 401


def test_off_the_platform_the_routes_are_the_applications(monkeypatch):
    app = make_app()
    app.config["open"] = True

    assert app.test_client().get("/__ske/health").status_code == 404


def test_the_celery_app_comes_from_the_extension_when_nothing_names_it(platform, monkeypatch, celery_app):
    monkeypatch.delenv("SKE_CELERY_APP")
    app = make_app()
    app.extensions["celery"] = celery_app

    assert app.extensions["ske"].celery_app(app) is celery_app


def test_an_explicit_celery_app_wins_over_the_environment_and_the_extension(platform, monkeypatch):
    explicit = Celery("explicit", set_as_current=False)
    app = make_app(celery=explicit)
    app.extensions["celery"] = Celery("other", set_as_current=False)

    assert app.extensions["ske"].celery_app(app) is explicit


def test_the_environment_wins_over_the_extension(platform, celery_app):
    app = make_app()
    app.extensions["celery"] = Celery("other", set_as_current=False)

    assert app.extensions["ske"].celery_app(app) is celery_app


def test_flask_ske_schedule_sends_the_due_entries(app, sqs_sent, monkeypatch):
    import ske_bridge.celery.scheduler as scheduler
    from tests.test_scheduler import MONDAY_0930_PARIS

    monkeypatch.setattr(scheduler, "current_minute", lambda: MONDAY_0930_PARIS)

    result = app.test_cli_runner().invoke(args=["ske", "schedule"])

    assert result.exit_code == 0, result.output
    assert "sent every-five (tests.record)" in result.output
    assert len(sqs_sent) == 3


def test_flask_ske_schedule_exits_non_zero_on_a_failed_send(app, celery_app, monkeypatch):
    monkeypatch.setitem(celery_app.conf, "beat_schedule", {"bad": {"task": "tests.record", "schedule": 60, "options": {"queue": "nowhere"}}})

    result = app.test_cli_runner().invoke(args=["ske", "schedule"])

    assert result.exit_code == 1
    assert "failed bad" in result.output


FORWARDED = {
    "Host": "app.example.com",
    "X-Forwarded-For": "203.0.113.9, 198.51.100.7",
    "X-Forwarded-Proto": "https",
    "X-Forwarded-Host": "app.example.com",
    "X-Forwarded-Port": "443",
}


def seen(app, headers):
    app.config["open"] = True
    return app.test_client().get("/client", headers=headers, environ_base={"REMOTE_ADDR": "127.0.0.1"}).json


def test_the_client_is_the_address_the_platform_appended(app):
    assert seen(app, FORWARDED) == {
        "remote_addr": "198.51.100.7",
        "scheme": "https",
        "host": "app.example.com",
        "url": "https://app.example.com/",
    }


def test_without_forwarded_headers_nothing_changes(app):
    assert seen(app, {"Host": "app.example.com"})["remote_addr"] == "127.0.0.1"


def test_off_the_platform_forwarded_headers_are_not_trusted():
    assert seen(make_app(), FORWARDED) == {
        "remote_addr": "127.0.0.1",
        "scheme": "http",
        "host": "app.example.com",
        "url": "http://app.example.com/",
    }


@pytest.mark.parametrize("path", [
    "/static/.secret",
    "/static/.hidden/app.css",
    "/static/settings.py",
    "/static/cache.PYC",
    "/static/run.sh",
    "/admin/static/.secret",
    "/admin/static/settings.py",
])
def test_the_static_route_refuses_what_the_platform_refuses(app, path):
    app.config["open"] = True

    assert app.test_client().get(path).status_code == 404


@pytest.mark.parametrize("path", ["/static/app.css", "/admin/static/app.css"])
def test_the_static_route_still_serves_assets(app, path):
    app.config["open"] = True

    response = app.test_client().get(path)

    assert response.status_code == 200
    assert response.data == b"body {}\n"


def test_off_the_platform_the_static_route_is_flasks_own():
    app = make_app()
    app.config["open"] = True

    assert app.test_client().get("/static/settings.py").status_code == 200


@pytest.fixture
def mail_service(platform, monkeypatch):
    monkeypatch.setenv("MAIL_HOST", "127.0.0.1")
    monkeypatch.setenv("MAIL_PORT", "2525")


MAIL_CONFIG = {
    "MAIL_SERVER": "smtp.example.com",
    "MAIL_PORT": 587,
    "MAIL_USE_TLS": True,
    "MAIL_USE_SSL": False,
    "MAIL_USERNAME": "app",
    "MAIL_PASSWORD": "secret",
}


def mail_config(app):
    return {key: app.config.get(key) for key in MAIL_CONFIG}


def app_with_mail_config():
    app = Flask("tests")
    app.config.update(MAIL_CONFIG)
    return app


def test_mail_goes_to_the_platforms_mail_service(mail_service):
    app = app_with_mail_config()
    SkeBridge(app)

    assert mail_config(app) == {
        "MAIL_SERVER": "127.0.0.1",
        "MAIL_PORT": 2525,
        "MAIL_USE_TLS": False,
        "MAIL_USE_SSL": False,
        "MAIL_USERNAME": None,
        "MAIL_PASSWORD": None,
    }


def test_a_mail_extension_initialised_first_is_pointed_at_the_service_too(mail_service):
    from types import SimpleNamespace

    app = app_with_mail_config()
    # The state Flask-Mail and Flask-Mailman keep from the config they read
    # when initialised.
    own = {"server": "smtp.example.com", "port": 587, "use_tls": True, "use_ssl": False, "username": "app", "password": "secret"}
    app.extensions["mail"] = SimpleNamespace(**own)
    app.extensions["mailman"] = SimpleNamespace(**own, backend="smtp")

    SkeBridge(app)

    for name in ("mail", "mailman"):
        state = vars(app.extensions[name])
        assert (state["server"], state["port"], state["use_tls"], state["username"], state["password"]) == ("127.0.0.1", 2525, False, None, None)


def test_a_mailman_backend_that_does_not_send_over_smtp_is_left_alone(mail_service):
    from types import SimpleNamespace

    app = app_with_mail_config()
    own = {"server": "smtp.example.com", "port": 587, "use_tls": True, "use_ssl": False, "username": "app", "password": "secret"}
    app.extensions["mailman"] = SimpleNamespace(**own, backend="console")

    SkeBridge(app)

    assert app.extensions["mailman"].server == "smtp.example.com"


def test_without_a_mail_service_the_mail_config_is_the_apps(platform):
    app = app_with_mail_config()
    SkeBridge(app)

    assert mail_config(app) == MAIL_CONFIG


def test_off_the_platform_mail_settings_are_the_apps(monkeypatch):
    monkeypatch.setenv("MAIL_HOST", "127.0.0.1")
    monkeypatch.setenv("MAIL_PORT", "2525")
    app = app_with_mail_config()
    SkeBridge(app)

    assert mail_config(app) == MAIL_CONFIG


def test_flasks_mail_reaches_the_service_over_plain_smtp_without_login(mail_service, smtp_catcher, monkeypatch):
    import smtplib

    monkeypatch.setenv("MAIL_PORT", str(smtp_catcher.port))
    app = app_with_mail_config()
    SkeBridge(app)
    config = app.config

    with smtplib.SMTP(config["MAIL_SERVER"], config["MAIL_PORT"], timeout=5) as smtp:
        if config["MAIL_USERNAME"]:
            smtp.login(config["MAIL_USERNAME"], config["MAIL_PASSWORD"])
        smtp.sendmail("app@example.com", ["user@example.com"], "Subject: hi\r\n\r\nhello")

    assert b"Subject: hi" in smtp_catcher.messages[0]


@pytest.fixture
def down(platform, monkeypatch):
    monkeypatch.setenv("MAINTENANCE_MODE", "true")
    monkeypatch.setenv("MAINTENANCE_MODE_SECRET", "preview-secret")


def site_client(app):
    app.config["open"] = True
    return app.test_client()


HTTPS = {"base_url": "https://app.example.com", "headers": {"X-Forwarded-Proto": "https"}}


def test_maintenance_answers_503_with_the_apps_template(down):
    response = site_client(make_app()).get("/", **HTTPS)

    assert response.status_code == 503
    assert response.data.strip() == b"<h1>Back soon</h1>"


def test_maintenance_falls_back_to_a_minimal_page(down):
    from ske_bridge.maintenance import DEFAULT_PAGE

    app = Flask("tests", template_folder=str(STATIC))
    SkeBridge(app)

    response = app.test_client().get("/")

    assert response.status_code == 503 and response.data == DEFAULT_PAGE
    assert response.content_type == "text/html; charset=utf-8"


def test_maintenance_keeps_readiness_and_the_queue(down, publish):
    client = site_client(make_app())

    assert client.get("/__ske/health").status_code == 200
    assert queue(client, [record(publish("tests.add", (1, 1)))]).json["outcomes"][0]["deleted"]


def test_the_secret_sets_a_cookie_that_lets_the_visitor_in(down):
    client = site_client(make_app())

    bypass = client.get("/preview-secret", **HTTPS)

    assert bypass.status_code == 302 and bypass.headers["Location"] == "/"
    assert "Secure" in bypass.headers["Set-Cookie"]
    assert client.get("/", **HTTPS).data == b"home"


def test_the_request_id_reaches_the_logs(platform):
    from ske_bridge.logging import request_id

    app = make_app()

    @app.route("/request-id")
    def logged():
        return request_id.get() or ""

    response = site_client(app).get("/request-id", headers={"X-Request-Id": "req-9"})

    assert response.data == b"req-9"


def test_flasks_own_log_handler_is_removed_so_records_are_written_once(platform):
    from flask.logging import default_handler

    app = Flask("own-handler")
    app.logger.addHandler(default_handler)
    SkeBridge(app)

    assert default_handler not in app.logger.handlers


def test_plain_logs_keep_flasks_handler(platform, monkeypatch):
    from flask.logging import default_handler

    monkeypatch.setenv("SKE_LOG_FORMAT", "plain")
    app = Flask("plain-logs")
    app.logger.addHandler(default_handler)
    SkeBridge(app)

    assert default_handler in app.logger.handlers
