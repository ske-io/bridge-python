# ske-bridge

Runs a Django or Flask application on SKE: it answers the platform's internal routes inside the application, runs delivered Celery tasks, and turns the Celery beat schedule into the command the platform runs every minute.

Everything is inert off the platform (when neither `SKE_PROJECT_TYPE` nor `SKE_ENVIRONMENT` is set), so the package is safe to install unconditionally.

## Install

```bash
pip install "ske-bridge[django,celery]"   # or [flask,celery]
```

Python 3.11–3.14. The `celery` extra installs `celery[sqs]`; leave it out if the application has no background tasks.

## Django

```python
# settings.py
import ske_bridge.django

INSTALLED_APPS = [
    # ...
    "ske_bridge.django",
]

DATABASES = ske_bridge.django.database(default="sqlite:///db.sqlite3")
```

On the platform the app puts its middleware at the front of `MIDDLEWARE`, so `urls.py` needs no change and host validation, CSRF and authentication never see the platform's requests. List `ske_bridge.django.middleware.InternalRoutesMiddleware` yourself to choose its position.

`database()` turns `DATABASE_URL` into `DATABASES`: `postgres://`, `postgresql://`, `mysql://`, `mariadb://` and `sqlite://` URLs, credentials percent-decoded, query parameters into `OPTIONS` (`?sslmode=require`). It takes `conn_max_age` and `conn_health_checks`. With neither `DATABASE_URL` nor `default` it returns `{}`, Django's setting for no database, which also lets `collectstatic` run at build time.

On the platform the middleware also sets `REMOTE_ADDR` to the client's address, and the app sets `SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")` unless your settings set it, so `request.is_secure()` and `build_absolute_uri()` report HTTPS. Both are safe there and only there; see [Client address and scheme](#client-address-and-scheme).

## Flask

```python
from flask import Flask
from ske_bridge.flask import SkeBridge

app = Flask(__name__)
SkeBridge(app)

# or, with an application factory
ske = SkeBridge()

def create_app():
    app = Flask(__name__)
    ske.init_app(app)
    return app
```

On the platform the extension wraps `app.wsgi_app`, so the internal routes are answered before Flask's routing and `before_request` hooks. It also applies Werkzeug's `ProxyFix` for one trusted proxy, so `request.remote_addr`, `request.scheme`, `request.host` and external URLs are the client's; see [Client address and scheme](#client-address-and-scheme). Don't add a `ProxyFix` of your own for the platform. Flask's static routes (the app's and each blueprint's) answer 404 for dotfiles, files in dot-directories and source files (`.py`, `.pyc`, `.sh`, `.php` and the like), as the platform's own static file server does; off the platform they serve as usual. Each Celery task runs inside an application context.

## Client address and scheme

Your app listens on loopback behind the container's entry process, so every connection it sees comes from `127.0.0.1`. The platform forwards each request with:

- `X-Forwarded-For`: the chain the client sent, with the address the platform saw the request come from appended last. Only that last entry is trusted, so a client can't choose its own address by sending the header.
- `X-Forwarded-Proto`, `X-Forwarded-Host`, `X-Forwarded-Port`: replaced on every request with the scheme, host and port the client connected with.
- `Forwarded`, `X-Real-IP` and `X-Forwarded-Prefix`: removed.

The bridge trusts exactly that one hop, and only on the platform; off it, nothing is changed, because the same headers would be whatever a client sent. If a CDN sits in front of your app, the last entry is the CDN's address, and reading the client from it is your app's own trusted-proxy configuration, as it would be behind any other proxy.

## Maintenance mode

Turn maintenance mode on for an environment in the SKE dashboard. The platform redeploys it with `MAINTENANCE_MODE=true`, and every web request is answered `503 Service Unavailable` with your `503.html` template (Django's template loaders, or Flask's `templates/` folder), or a minimal page when you have none. The readiness check and queued tasks keep working; the scheduler sends no beat entries, except those marked to run anyway:

```python
app.conf.beat_schedule = {
    "cleanup": {"task": "proj.tasks.cleanup", "schedule": 3600, "options": {"ske_even_in_maintenance": True}},
}
```

Set `MAINTENANCE_MODE_SECRET` to look at the site while it's down: visiting `https://your-site/<secret>` sets a cookie and redirects to `/`, and that browser is let in for 12 hours. The cookie is signed with the secret, so changing the secret ends every one handed out.

## Logs

On the platform your app's logs are written to stderr as one JSON line per record, which the logs page reads into level, message, context and exception, with its traceback. Records keep the logger's name, the fields you pass with `extra=`, and the request's id as `request_id`. The root logger gets a stderr handler in this format, other loggers' console handlers (Django's, Flask's, gunicorn's) take the format too, and a console handler on a logger whose records reach the root logger anyway is removed, so nothing is written twice. Levels stay as you set them.

Set `SKE_LOG_FORMAT=plain` to keep your logging exactly as you configured it.

## Mail

When the environment has the platform's mail service attached, the platform runs an SMTP server in the container that catches every mail your app sends, so none reaches a real inbox, and sets `MAIL_HOST` and `MAIL_PORT` to it. It speaks plain SMTP, with no TLS and no login. On the platform the bridge points your app at it:

- Django: when `EMAIL_BACKEND` is the SMTP backend (the default), `EMAIL_HOST` and `EMAIL_PORT` are set to the service, `EMAIL_USE_TLS` and `EMAIL_USE_SSL` to `False`, and `EMAIL_HOST_USER` and `EMAIL_HOST_PASSWORD` are cleared. Other backends (console, locmem, a provider's API) are left alone.
- Flask: `MAIL_SERVER`, `MAIL_PORT`, `MAIL_USE_TLS=False`, `MAIL_USE_SSL=False`, and `MAIL_USERNAME` and `MAIL_PASSWORD` cleared, which Flask-Mail and Flask-Mailman read. An extension initialised before `SkeBridge(app)` is updated too. Flask-Mailman with a non-SMTP backend is left alone.

These override your own SMTP server settings: the service is attached to keep mail from going out, and your real server would let it out. Without the service, and off the platform, your settings are used as they are. An app that sends with `smtplib` directly should connect to `MAIL_HOST` and `MAIL_PORT` itself, without `starttls()` or `login()`.

## Celery

Point Celery at the platform's queue after the application's own configuration:

```python
# proj/celery.py
import ske_bridge.celery
from celery import Celery

app = Celery("proj")
app.config_from_object("django.conf:settings", namespace="CELERY")
ske_bridge.celery.configure(app)
app.autodiscover_tasks()
```

On AWS that is the environment's SQS queues: `broker_url = "sqs://"`, the queues added to `broker_transport_options["predefined_queues"]` (the application's other transport options and predefined queues are kept) and `task_default_queue` set. Credentials come from the function's role.

On GCP it publishes Celery envelopes to the environment's Cloud Tasks queues through the bridge's Kombu transport. The platform sets `SKE_GCP_PROJECT`, `SKE_REGION`, `SKE_QUEUE_PREFIX` and `SKE_QUEUES`; Cloud Run's metadata server supplies the access token. The queues' HTTP targets select and authenticate the receiving service. No Google SDK or Celery worker is needed. A countdown becomes a Cloud Tasks scheduled task, at most 30 days ahead. An unknown queue or a longer countdown fails to publish. Off the platform it changes nothing.

GCP currently has no dead-letter store in the platform. A task that fails permanently is acknowledged after Celery's failure handling, so use a Celery result backend to retain its failure. AWS retains it in the SQS dead-letter queue.

Every queue the environment has is known to Celery, from `SQS_QUEUES` on AWS and `SKE_QUEUES` on GCP, with `default` as the default queue. Route a task to another one as usual, with `queue="emails"` on the task or the call, or in `task_routes`. A task routed to a queue the environment doesn't have fails to send rather than go to a queue nothing reads.

The platform delivers each message to the application; no worker process runs. The bridge finds the Celery app, in order, from `SkeBridge(app, celery=...)`, from `SKE_CELERY_APP` (or `CELERY_APP`) named as for `celery -A` (`proj`, `proj.celery:app`), from Flask's `app.extensions["celery"]`, and last from the app the project created, which Django's usual `proj/__init__.py` import does. With none of these the queue answers 500 and the platform delivers the batch again, rather than run it against Celery's empty default app.

A delivered task runs through Celery's own tracer, so signals, result backends, links and chains behave as under `celery worker`. What becomes of the message:

| The task | The message |
|---|---|
| returns, or raises `Ignore` | acknowledged |
| calls `self.retry()`, or retries through `autoretry_for` | the retry publishes a new message (its arguments, its countdown as an eta, `retries + 1`) and this one is acknowledged, as under a worker; if that publish fails, this one is delivered again |
| raises anything else, or runs out of retries | kept in the queue's dead-letter queue |
| raises `Reject(requeue=True)` | delivered again |
| has an `eta`/`countdown` still ahead | held unrun until then, at most 12 hours at a time (SQS's limit), and checked again on each delivery |
| has `expires` in the past | revoked unrun (`task_revoked` is sent and the result backend records it) |
| succeeded, but its result could not be saved | acknowledged, with the error logged: the task already ran |

The retry count is the message's own `retries`, so a delivery held for an eta never uses up a retry.

Retries and backoff are Celery's per-task settings: `max_retries`, `default_retry_delay`, `autoretry_for`, `retry_backoff`. The queue's own `tries` and `backoff` settings are not applied to Celery tasks. `tries` is the SQS queue's `maxReceiveCount`, which counts deliveries of one message; each Celery retry is a new message, so it starts that count again. The count matters only for a message that keeps being delivered without an outcome (a crash, a timeout of the whole invocation) and for a message held for a long eta, which is delivered once per 12 hours; set `tries` high enough for the longest eta a task uses.

Time limits are a worker's: at a task's soft limit it gets `SoftTimeLimitExceeded`, and at its hard limit `TimeLimitExceeded`. The batch has one deadline as well, a few seconds before the platform cuts it off (the time the platform gives, or else the queue's timeout counted from arrival): each task's hard limit is cut to the time left. A task stopped by that deadline rather than its own limit is delivered again in a later invocation, with no failure handling run, and so is a message reached with too little time left. Interrupting a task needs gunicorn's default sync worker, which runs requests on the main thread.

Only task protocol 2 (Celery's default since 4.0) is supported, and the message body is decoded with the app's `accept_content`.

## Beat schedule

No beat process runs. Once a minute the platform runs the scheduler command for your framework:

- Django: `python manage.py ske_schedule`
- Flask: `flask ske schedule`

Each run sends the `beat_schedule` entries due in the minute it was scheduled for (the platform passes it as `SKE_SCHEDULED_AT`; without it, the current minute) (including those added with `add_periodic_task`) and exits; it exits non-zero if an entry could not be sent. Due is decided from the minute alone:

- `crontab` entries are due when the minute matches, in the app's `timezone`.
- Interval entries (seconds, `timedelta`) are due when the minutes since Monday 1970-01-05 00:00 UTC are a multiple of the interval, rounded down to whole minutes: hourly at the top of the hour, daily at 00:00 UTC, weekly on Mondays at 00:00 UTC; 90 seconds runs every minute. An interval under a minute runs once a minute, with a warning in the log.
- Other schedules (`solar`, custom classes) are due when their next event falls in the minute, its start included and its end excluded, so an event is sent by one tick only.

Daylight saving time applies to `crontab` entries in a timezone that has it. When clocks go back, the repeated hour's minutes match twice, so a `crontab(hour=1, minute=30)` entry runs twice that night. When clocks go forward, the skipped hour never happens, so an entry in it does not run that day. Intervals count UTC minutes and are not affected. Set `timezone = "UTC"` to avoid both. Because each run uses the minute it was scheduled for, a run that starts late on either night still decides by that minute, not by when it started.

The schedule cannot be finer than a minute. A run that starts late or is retried sends the entries of the minute it was scheduled for, not of the minute it started in. A tick delivered twice still sends its entries twice, so a task that must never run twice guards itself, as it would under more than one beat.

## Commands

New Django environments start with `python manage.py migrate --noinput` as their deploy hook. Change or disable it in the environment settings when the application needs a different migration step or has no database. Flask environments start without deploy hooks.

Commands from the CLI or the console (`python manage.py migrate`, `flask db upgrade`) run as plain processes in the container; they need nothing from the bridge.

## Maintenance and logs

On the platform, `MAINTENANCE_MODE=true` pauses web requests with a 503 response. Internal queue and health routes keep working, as does the application's `/up` readiness route. Visiting `/{MAINTENANCE_MODE_SECRET}` sets a signed, HTTP-only bypass cookie valid for 12 hours; changing the secret invalidates existing cookies. Responses are not cached.

Console log handlers write JSON records with the message, severity, logger name and exception frames, so errors and tracebacks appear correctly in the SKE logs viewer. File handlers keep their own format. Both behaviors are inactive off the platform.

## Environment

Every process inherits the platform's variables in `os.environ`, so there is nothing to normalise. Dotenv loaders leave a variable that is already set alone unless told to override it, so a `.env` in the image cannot replace what the platform set.
