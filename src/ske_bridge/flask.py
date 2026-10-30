"""The Flask integration::

    from ske_bridge.flask import SkeBridge

    app = Flask(__name__)
    SkeBridge(app)          # or ske.init_app(app) in an app factory

On the platform it wraps the app's WSGI callable with the internal routes
(see ske_bridge.routes), so they are answered before Flask's routing and
``before_request`` hooks, and with Werkzeug's ProxyFix trusting the one proxy
hop the platform puts in front of the app, so ``request.remote_addr``, the
scheme and the host are the client's (see ske_bridge.forwarded). Its static
routes refuse dotfiles and source files, as the platform's static server does
(see ske_bridge.static). Everywhere it adds ``flask ske schedule``, the
platform's scheduler command.

The Celery app is ``celery=``, else what ``SKE_CELERY_APP`` or ``CELERY_APP``
names, else ``app.extensions["celery"]`` as Flask's Celery guide sets it,
else the app the project made current; see ske_bridge.celery.find_app. Each task runs inside an application context.
"""

from __future__ import annotations

from typing import Any

import click
from flask import Flask, abort, current_app, request
from flask.cli import with_appcontext

from ske_bridge import environment
from ske_bridge.forwarded import TRUSTED_HOPS
from ske_bridge.mail import mail_service
from ske_bridge.routes import Routes
from ske_bridge.static import refused
from ske_bridge.wsgi import InternalRoutes


class SkeBridge:
    def __init__(self, app: Flask | None = None, celery: Any = None):
        self.celery = celery
        if app is not None:
            self.init_app(app)

    def init_app(self, app: Flask) -> None:
        app.extensions["ske"] = self
        app.cli.add_command(ske)

        if environment.running_on_platform():
            from werkzeug.middleware.proxy_fix import ProxyFix

            hops = TRUSTED_HOPS
            app.wsgi_app = InternalRoutes(
                ProxyFix(app.wsgi_app, x_for=hops, x_proto=hops, x_host=hops, x_port=hops, x_prefix=0),
                Routes(lambda: self._runner(app)),
                maintenance_page=lambda environ: _maintenance_page(app, environ),
            )
            # First, so no hook of the application's answers for a file the
            # platform refused.
            app.before_request_funcs.setdefault(None, []).insert(0, _refuse_static)
            use_mail_service(app)
            use_structured_logs(app)

    def celery_app(self, app: Flask) -> Any:
        from ske_bridge.celery import find_app

        return find_app(self.celery, app.extensions.get("celery"))

    def _runner(self, app: Flask) -> Any:
        from ske_bridge.celery.runner import QueueRunner

        return QueueRunner(self.celery_app(app), around_job=app.app_context)


def use_structured_logs(app: Flask) -> None:
    """Writes the app's logs in the platform's format (see
    ske_bridge.logging). Flask's own handler on ``app.logger`` is removed
    once the root logger writes them, or each record would be written
    twice."""
    from flask.logging import default_handler

    from ske_bridge.logging import configure, enabled

    if not enabled():
        return
    configure()
    app.logger.removeHandler(default_handler)


def _maintenance_page(app: Flask, environ: dict[str, Any]) -> bytes | None:
    """The app's ``503.html`` template, rendered for the request, or None
    when it has none."""
    from flask import render_template
    from jinja2 import TemplateNotFound

    from ske_bridge.maintenance import TEMPLATE

    with app.request_context(environ):
        try:
            return render_template(TEMPLATE).encode()
        except TemplateNotFound:
            return None


def use_mail_service(app: Flask) -> None:
    """Points Flask-Mail and Flask-Mailman at the platform's mail service when
    one is attached (see ske_bridge.mail): ``MAIL_SERVER`` and ``MAIL_PORT``,
    no TLS or SSL, and no login. This overrides the app's own mail server.

    Either extension reads the config once, when it is initialised, so one
    already initialised for this app has its settings changed in place too;
    SkeBridge can be set up before or after it. Flask-Mailman set to a
    backend other than SMTP is left alone."""
    service = mail_service()
    if service is None:
        return

    server, port = service
    settings = {"server": server, "port": port, "use_tls": False, "use_ssl": False, "username": None, "password": None}
    app.config.update({f"MAIL_{key.upper()}": value for key, value in settings.items()})

    for name in ("mail", "mailman"):
        state = app.extensions.get(name)
        if state is None or not _sends_over_smtp(getattr(state, "backend", None)):
            continue
        for key, value in settings.items():
            if hasattr(state, key):
                setattr(state, key, value)


def _sends_over_smtp(backend: Any) -> bool:
    """Flask-Mail has no backends; Flask-Mailman names its SMTP one ``smtp``
    or by its import path."""
    return backend is None or backend == "smtp" or (isinstance(backend, str) and backend.endswith("smtp.EmailBackend"))


def _refuse_static() -> None:
    """Answers 404 for a dotfile or source file under the app's or a
    blueprint's static route, as the platform does; see ske_bridge.static."""
    endpoint = request.endpoint or ""
    if endpoint == "static" or endpoint.endswith(".static"):
        if refused(str((request.view_args or {}).get("filename", ""))):
            abort(404)


@click.group(help="SKE platform commands.")
def ske() -> None:
    pass


@ske.command(help="Send the Celery beat schedule's entries that are due this minute.")
@with_appcontext
def schedule() -> None:
    from ske_bridge.celery.scheduler import tick

    bridge: SkeBridge = current_app.extensions["ske"]
    result = tick(bridge.celery_app(current_app), echo=click.echo)
    if not result.ok:
        raise click.ClickException(f"{len(result.failed)} scheduled task(s) could not be sent")
