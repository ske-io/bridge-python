import json
import time
from pathlib import Path

import pytest
from celery import Celery
from flask import Blueprint, Flask, current_app

from ske_bridge.celery import NoCeleryApp
from ske_bridge.flask import SkeBridge
from ske_bridge.jobs import job
from tests.conftest import define_task, record


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


def test_jobs_run_inside_an_app_context_beside_tasks(app, publish):
    import uuid

    from ske_bridge.jobs import job
    from ske_bridge.jobs.message import JobMessage, encode
    from tests.celery_app import calls

    seen = []

    @job("flask-job")
    async def handler(payload, ctx):
        seen.append((payload, current_app.name))

    body = encode(JobMessage("flask-job", [1], str(uuid.uuid4())))
    response = queue(app.test_client(), [record(publish("tests.add", (2, 2)), message_id="task"), record(body, message_id="job")])

    assert [(o["message_id"], o["deleted"]) for o in response.json["outcomes"]] == [("task", True), ("job", True)]
    assert seen == [([1], "tests")]
    assert calls == [("add", 2, 2)]


def test_jobs_run_without_a_celery_app(platform, monkeypatch):
    import uuid

    from ske_bridge.jobs import job
    from ske_bridge.jobs.message import JobMessage, encode

    monkeypatch.delenv("SKE_CELERY_APP")
    monkeypatch.setattr("celery._state._tls.current_app", None, raising=False)
    monkeypatch.setattr("celery._state._get_active_apps", lambda: [])
    seen = []
    job("flask-only-job")(lambda payload, ctx: seen.append(current_app.name))

    response = queue(make_app().test_client(), [record(encode(JobMessage("flask-only-job", None, str(uuid.uuid4()))))])

    assert response.status_code == 200 and response.json["outcomes"][0]["deleted"]
    assert seen == ["tests"]


def test_a_job_dispatched_off_the_platform_runs_in_an_app_context(monkeypatch):
    import threading

    from flask import has_app_context

    from ske_bridge.jobs import job

    app = make_app()
    seen = []
    done = threading.Event()

    @job("off-platform")
    def handler(payload, ctx):
        seen.append((payload, current_app.name))
        done.set()

    assert not has_app_context()
    monkeypatch.setenv("SKE_QUEUE_SYNC", "1")
    handler.dispatch("sync")
    monkeypatch.delenv("SKE_QUEUE_SYNC")
    handler.dispatch("scheduled")

    assert done.wait(5)
    for _ in range(100):
        if len(seen) == 2:
            break
        threading.Event().wait(0.02)
    assert seen == [("sync", "tests"), ("scheduled", "tests")]
    assert app.name == "tests"


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


def test_flask_ske_schedule_runs_the_tasks_and_fails_with_one(monkeypatch):
    app = Flask("tests")
    SkeBridge(app)
    monkeypatch.setattr(SkeBridge, "celery_app", lambda self, app: (_ for _ in ()).throw(NoCeleryApp("none")))
    monkeypatch.setenv("SKE_SCHEDULED_AT", "2026-03-04T10:01:00Z")
    seen = []
    define_task("* * * * *", "native", lambda: seen.append(current_app.name))

    result = app.test_cli_runner().invoke(args=["ske", "schedule"])

    assert result.exit_code == 0, result.output
    assert result.output.startswith("ran native in ")
    # Tasks run in the app's context.
    assert seen == ["tests"]

    define_task("* * * * *", "broken", lambda: 1 / 0)
    result = app.test_cli_runner().invoke(args=["ske", "schedule"])
    assert result.exit_code == 1
    assert "1 scheduled task(s) failed: broken" in result.output


def test_flask_ske_schedule_waits_for_local_jobs(monkeypatch):
    app = Flask("tests")
    SkeBridge(app)
    monkeypatch.setattr(SkeBridge, "celery_app", lambda self, app: (_ for _ in ()).throw(NoCeleryApp("none")))
    seen = []

    @job("in-context")
    def in_context(payload, ctx):
        time.sleep(0.05)
        seen.append(current_app.name)

    define_task("* * * * *", "dispatches", lambda: in_context.dispatch())

    result = app.test_cli_runner().invoke(args=["ske", "schedule"])

    assert result.exit_code == 0, result.output
    assert seen == ["tests"]


def test_flask_ske_schedule_runs_each_task_in_an_app_context_of_its_own(monkeypatch):
    from flask import g

    app = Flask("tests")
    SkeBridge(app)
    monkeypatch.setattr(SkeBridge, "celery_app", lambda self, app: (_ for _ in ()).throw(NoCeleryApp("none")))
    seen = []

    def first():
        g.user = "set by the first task"

    def second():
        seen.append((current_app.name, getattr(g, "user", None)))

    define_task("* * * * *", "first", first)
    define_task("* * * * *", "second", second)

    result = app.test_cli_runner().invoke(args=["ske", "schedule"])

    assert result.exit_code == 0, result.output
    assert seen == [("tests", None)]


def test_flask_ske_schedule_without_tasks_or_a_celery_app_names_both(monkeypatch):
    app = Flask("tests")
    SkeBridge(app)
    monkeypatch.setattr(SkeBridge, "celery_app", lambda self, app: (_ for _ in ()).throw(NoCeleryApp("none")))

    result = app.test_cli_runner().invoke(args=["ske", "schedule"])

    assert result.exit_code == 1
    assert "no scheduled tasks are defined" in result.output and "no Celery app is configured" in result.output


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
    monkeypatch.setenv("SKE_MAINTENANCE_BYPASS_TOKEN", BYPASS_TOKEN)


BYPASS_TOKEN = "6f1c0e9a4b7d2385f0a1c3e5b7d9f1a3c5e7092b4d6f8a1c3e5b7d9f1a3c5e70"


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


@pytest.mark.parametrize("secret", [None, "preview-secret"])
def test_the_platforms_maintenance_bypass_header_lets_the_request_in(down, monkeypatch, secret):
    if secret is None:
        monkeypatch.delenv("MAINTENANCE_MODE_SECRET")
    client = site_client(make_app())

    response = client.get("/", headers={"SKE-Maintenance-Bypass": BYPASS_TOKEN})

    assert response.status_code == 200
    assert response.data == b"home"
    assert "Set-Cookie" not in response.headers
    assert client.get("/").status_code == 503


@pytest.mark.parametrize("value", ["", "1", "true", BYPASS_TOKEN[:-1], BYPASS_TOKEN + "0", BYPASS_TOKEN.upper(), f"{BYPASS_TOKEN}, {BYPASS_TOKEN}", "é"])
def test_other_maintenance_bypass_header_values_do_not_let_the_request_in(down, value):
    response = site_client(make_app()).get("/", headers={"SKE-Maintenance-Bypass": value})

    assert response.status_code == 503


@pytest.mark.parametrize(("token", "value"), [(None, "1"), (None, ""), ("", ""), ("", "1")])
def test_the_maintenance_bypass_header_is_ignored_without_a_platform_token(down, monkeypatch, token, value):
    # A platform that sets no token may also pass along a client's copy of
    # the header, so the header alone must never let a request in.
    monkeypatch.delenv("SKE_MAINTENANCE_BYPASS_TOKEN")
    if token is not None:
        monkeypatch.setenv("SKE_MAINTENANCE_BYPASS_TOKEN", token)

    response = site_client(make_app()).get("/", headers={"SKE-Maintenance-Bypass": value})

    assert response.status_code == 503


def test_the_maintenance_bypass_header_leaves_the_secret_path_to_the_app(down):
    app = make_app()

    @app.route("/preview-secret")
    def secret_page():
        return "secret page"

    response = site_client(app).get("/preview-secret", headers={"SKE-Maintenance-Bypass": BYPASS_TOKEN})

    assert response.status_code == 200
    assert response.data == b"secret page"
    assert "Set-Cookie" not in response.headers


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


def test_flask_caching_reads_the_platform_prefix(platform, monkeypatch):
    monkeypatch.setenv("SKE_CACHE_PREFIX", "shop-pr-1:cache:")
    app = Flask("tests")
    app.config["CACHE_KEY_PREFIX"] = "myapp_"

    SkeBridge(app)

    assert app.config["CACHE_KEY_PREFIX"] == "shop-pr-1:cache:myapp_"


def test_a_cache_initialised_first_gets_the_prefix_too(platform, monkeypatch):
    from types import SimpleNamespace

    monkeypatch.setenv("SKE_CACHE_PREFIX", "shop-pr-1:cache:")
    app = Flask("tests")
    # Flask-Caching keeps one backend per Cache object, built from the config
    # when initialised; the shared-server backends hold the prefix as key_prefix.
    redis = SimpleNamespace(key_prefix="flask_cache_")
    simple = SimpleNamespace()
    app.extensions["cache"] = {object(): redis, object(): simple}

    SkeBridge(app)

    assert redis.key_prefix == "shop-pr-1:cache:flask_cache_"
    assert vars(simple) == {}


def test_two_previews_get_different_cache_prefixes(platform, monkeypatch):
    prefixes = []
    for prefix in ("shop-pr-1:cache:", "shop-pr-2:cache:"):
        monkeypatch.setenv("SKE_CACHE_PREFIX", prefix)
        app = Flask("tests")
        SkeBridge(app)
        prefixes.append(app.config["CACHE_KEY_PREFIX"])

    assert prefixes == ["shop-pr-1:cache:", "shop-pr-2:cache:"]


def test_the_cache_config_is_left_alone_without_a_prefix(platform, monkeypatch):
    monkeypatch.delenv("SKE_CACHE_PREFIX", raising=False)
    app = Flask("tests")

    SkeBridge(app)

    assert "CACHE_KEY_PREFIX" not in app.config


def test_a_callable_cache_prefix_keeps_working(platform, monkeypatch):
    from types import SimpleNamespace

    monkeypatch.setenv("SKE_CACHE_PREFIX", "shop-pr-1:cache:")
    app = Flask("tests")
    app.config["CACHE_KEY_PREFIX"] = lambda: "tenant:"
    redis = SimpleNamespace(key_prefix=lambda: "tenant:")
    app.extensions["cache"] = {object(): redis}

    SkeBridge(app)

    assert app.config["CACHE_KEY_PREFIX"]() == "shop-pr-1:cache:tenant:"
    assert redis.key_prefix() == "shop-pr-1:cache:tenant:"


def register_backend(app, backend):
    """What Flask-Caching does when a Cache is initialised, after merging its
    own config over app.config: it stores the backend it built."""
    app.extensions.setdefault("cache", {})
    app.extensions["cache"][object()] = backend


def test_a_cache_initialised_after_the_bridge_gets_the_prefix_as_it_is_built(platform, monkeypatch):
    from types import SimpleNamespace

    monkeypatch.setenv("SKE_CACHE_PREFIX", "shop-pr-1:cache:")
    app = Flask("tests")
    SkeBridge(app)

    # Outside any application context, as Cache(app, config=...) at import time.
    outside = SimpleNamespace(key_prefix="own:")
    register_backend(app, outside)
    # Inside one that was pushed before the backend existed.
    with app.app_context():
        inside = SimpleNamespace(key_prefix="own:")
        register_backend(app, inside)
        assert inside.key_prefix == "shop-pr-1:cache:own:"

    assert outside.key_prefix == "shop-pr-1:cache:own:"


def test_setting_up_the_bridge_again_keeps_one_prefix(platform, monkeypatch):
    from types import SimpleNamespace

    monkeypatch.setenv("SKE_CACHE_PREFIX", "shop-pr-1:cache:")
    app = Flask("tests")
    SkeBridge(app)
    SkeBridge(app)
    redis = SimpleNamespace(key_prefix="own:")
    register_backend(app, redis)
    SkeBridge(app)

    assert redis.key_prefix == "shop-pr-1:cache:own:"
    assert app.config["CACHE_KEY_PREFIX"] == "shop-pr-1:cache:"


def test_the_bridge_holds_no_reference_to_a_discarded_app(platform, monkeypatch):
    import gc
    import weakref

    monkeypatch.setenv("SKE_CACHE_PREFIX", "shop-pr-1:cache:")
    app = Flask("tests")
    SkeBridge(app)
    ref = weakref.ref(app)

    del app
    gc.collect()

    assert ref() is None
