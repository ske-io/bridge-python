# ske-bridge

Runs a Django, Flask or any other WSGI or ASGI application on SKE: it answers the platform's internal routes inside the application, runs background jobs and delivered Celery tasks, and turns the Celery beat schedule into the command the platform runs every minute.

Everything is inert off the platform (when neither `SKE_PROJECT_TYPE` nor `SKE_ENVIRONMENT` is set), so the package is safe to install unconditionally.

## Install

```bash
pip install "ske-bridge[django,jobs]"   # or [flask,jobs], or [django,celery] for Celery
```

Python 3.11–3.14. The `jobs` extra installs boto3, which sending [jobs](#jobs) on AWS needs. The `celery` extra installs `celery[sqs]`, which includes boto3. Leave both out if the application has no background work.

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

On the platform the extension wraps `app.wsgi_app`, so the internal routes are answered before Flask's routing and `before_request` hooks. It also applies Werkzeug's `ProxyFix` for one trusted proxy, so `request.remote_addr`, `request.scheme`, `request.host` and external URLs are the client's; see [Client address and scheme](#client-address-and-scheme). Don't add a `ProxyFix` of your own for the platform. Flask's static routes (the app's and each blueprint's) answer 404 for dotfiles, files in dot-directories and source files (`.py`, `.pyc`, `.sh`, `.php` and the like), as the platform's own static file server does; off the platform they serve as usual. Each job and each Celery task runs inside an application context.

## Any WSGI or ASGI app

For an app that is neither Django nor Flask (FastAPI, Starlette, Litestar, Falcon, Bottle, or a plain WSGI or ASGI callable), wrap it where your server loads it:

```python
# main.py
import ske_bridge.asgi   # ske_bridge.wsgi for a WSGI app

app = ske_bridge.asgi.wrap(FastAPI())
```

or name it in the server's import string, which leaves your code unchanged:

```python
# gunicorn.conf.py
wsgi_app = 'ske_bridge.asgi:wrap("main:app")'   # with an ASGI worker class
wsgi_app = 'ske_bridge.wsgi:wrap("main:app")'   # for a WSGI app
```

`wrap()` takes the app or a `module:attribute` import string. An import string that can't be imported fails with an `ImportError` naming it. Off the platform `wrap()` returns the app unchanged, and wrapping an app twice changes nothing. A Django app with `ske_bridge.django` installed, or a Flask app with `SkeBridge`, already has all of this and more, so `wrap()` refuses it with an error: serve it as it is. On the platform it adds:

- The internal routes, answered before your app's routing and middleware.
- Maintenance mode, with the minimal maintenance page (there is no template to look up); see [Maintenance mode](#maintenance-mode).
- The client's address, scheme and host from the trusted hop: `REMOTE_ADDR`, `wsgi.url_scheme` and `HTTP_HOST` in the WSGI environ, or `client`, `scheme`, the `host` header and `server` in the ASGI scope; see [Client address and scheme](#client-address-and-scheme). Don't add your own proxy-header middleware for the platform.
- The request id in your logs, and structured logs; see [Logs](#logs).

Lifespan events reach an ASGI app untouched, so startup and shutdown handlers run as usual. WebSockets reach it with the client's address and scheme. During maintenance a new WebSocket is refused unless it is let in the same ways a request is: with a `503` and the maintenance page when the server can send one, otherwise by closing it before it is accepted, which the server answers with `403`. Connections opened before maintenance began are ended by the redeploy.

Delivered [jobs](#jobs) run once the app's import has defined them. Delivered Celery tasks run through Celery when `SKE_CELERY_APP` (or `CELERY_APP`) names the Celery app, as for `celery -A`; see [Celery](#celery). With no job defined and no Celery app named, the queue answers `500` saying nothing runs queued jobs, and the platform delivers the batch again. An ASGI app runs each batch in a thread of the bridge's own pool, so the event loop keeps serving requests and the readiness route answers at once while batches run.

Interrupting a sync job or a task needs the main thread, so under an ASGI app neither a sync job's time limit, nor Celery's time limits (`time_limit`, `soft_time_limit`), nor the batch's deadline stop it: it runs until it returns. A job or task that runs past the queue's timeout makes the platform time out the whole request, and the platform then delivers every message of that batch again, including those that already succeeded. Keep each one well inside the queue's timeout, and make it safe to run twice. A WSGI app served by gunicorn's sync worker has neither limitation, and async jobs are cancelled at their limit either way.

So for an ASGI app the platform runs delivered jobs and tasks in a separate process with `ske_bridge.wsgi:queue("main:app")` (gunicorn's `wsgi_app`, on sync workers), and the time limits and deadline apply there. It imports your app, so the jobs, Celery app and tasks its import sets up are defined, but never calls it: it answers only the internal routes, and `404` for every other path. It works for a WSGI app as well, and refuses an app with the Django or Flask integration, as `wrap()` does.

The mail service and the cache key prefix are environment variables (`MAIL_HOST`, `MAIL_PORT`, `SKE_CACHE_PREFIX`) such an app reads itself, as described under [Mail](#mail) and [Cache key prefix](#cache-key-prefix).

## Client address and scheme

Your app listens on loopback behind the container's entry process, so every connection it sees comes from `127.0.0.1`. The platform forwards each request with:

- `X-Forwarded-For`: the chain the client sent, with the address the platform saw the request come from appended last. Only that last entry is trusted, so a client can't choose its own address by sending the header.
- `X-Forwarded-Proto`, `X-Forwarded-Host`, `X-Forwarded-Port`: replaced on every request with the scheme, host and port the client connected with.
- `Forwarded`, `X-Real-IP` and `X-Forwarded-Prefix`: removed.

The bridge trusts exactly that one hop, and only on the platform; off it, nothing is changed, because the same headers would be whatever a client sent. If a CDN sits in front of your app, the last entry is the CDN's address, and reading the client from it is your app's own trusted-proxy configuration, as it would be behind any other proxy.

## Maintenance mode

Turn maintenance mode on for an environment in the SKE dashboard. The platform redeploys it with `MAINTENANCE_MODE=true`, and every web request is answered `503 Service Unavailable` with your `503.html` template (Django's template loaders, or Flask's `templates/` folder), or a minimal page when you have none. The readiness check, jobs and queued tasks keep working; the scheduler sends no beat entries, except those marked to run anyway:

```python
app.conf.beat_schedule = {
    "cleanup": {"task": "proj.tasks.cleanup", "schedule": 3600, "options": {"ske_even_in_maintenance": True}},
}
```

Set `MAINTENANCE_MODE_SECRET` to look at the site while it's down: visiting `https://your-site/<secret>` sets a cookie and redirects to `/`, and that browser is let in for 12 hours. The cookie is signed with the secret, so changing the secret ends every one handed out.

Requests from addresses on the environment's access list also reach the app normally during maintenance, without setting a bypass cookie or requiring `MAINTENANCE_MODE_SECRET`. The platform marks them with an `SKE-Maintenance-Bypass` header, after removing any copy a client sent, carrying a secret token it puts in `SKE_MAINTENANCE_BYPASS_TOKEN` when the instance starts. A request is let in only when the header equals the token, compared in constant time, and never when the token is unset.

## Logs

On the platform your app's logs are written to stderr as one JSON line per record, which the logs page reads into level, message, context and exception, with its traceback. Records keep the logger's name, the fields you pass with `extra=`, and the request's id as `request_id`. The root logger gets a stderr handler in this format, other loggers' console handlers (Django's, Flask's, gunicorn's) take the format too, and a console handler on a logger whose records reach the root logger anyway is removed, so nothing is written twice. Levels stay as you set them.

Set `SKE_LOG_FORMAT=plain` to keep your logging exactly as you configured it.

## Mail

When the environment has the platform's mail service attached, the platform runs an SMTP server in the container that catches every mail your app sends, so none reaches a real inbox, and sets `MAIL_HOST` and `MAIL_PORT` to it. It speaks plain SMTP, with no TLS and no login. On the platform the bridge points your app at it:

- Django: when `EMAIL_BACKEND` is the SMTP backend (the default), `EMAIL_HOST` and `EMAIL_PORT` are set to the service, `EMAIL_USE_TLS` and `EMAIL_USE_SSL` to `False`, and `EMAIL_HOST_USER` and `EMAIL_HOST_PASSWORD` are cleared. Other backends (console, locmem, a provider's API) are left alone.
- Flask: `MAIL_SERVER`, `MAIL_PORT`, `MAIL_USE_TLS=False`, `MAIL_USE_SSL=False`, and `MAIL_USERNAME` and `MAIL_PASSWORD` cleared, which Flask-Mail and Flask-Mailman read. An extension initialised before `SkeBridge(app)` is updated too. Flask-Mailman with a non-SMTP backend is left alone.

These override your own SMTP server settings: the service is attached to keep mail from going out, and your real server would let it out. Without the service, and off the platform, your settings are used as they are. An app that sends with `smtplib` directly should connect to `MAIL_HOST` and `MAIL_PORT` itself, without `starttls()` or `login()`.

## Cache key prefix

A preview environment shares its base environment's cache server. To keep their keys apart, the platform sets `SKE_CACHE_PREFIX` on each preview, and on the platform the bridge puts it before your own prefix:

- Django: the `KEY_PREFIX` of every alias in `CACHES`, so `KEY_PREFIX = "myapp"` becomes `<SKE_CACHE_PREFIX>myapp`. A cache another app used before this one was ready is updated too.
- Flask: Flask-Caching's `CACHE_KEY_PREFIX`, including a `Cache`'s own `config=`, whether it is set up before or after `SkeBridge(app)`. A callable prefix stays callable.

Without the variable, your settings are used as they are. Code that talks to Redis or Memcached directly, outside the framework's cache, should put `SKE_CACHE_PREFIX` before its keys itself.

## Jobs

```python
# shop/jobs.py
from ske_bridge.jobs import dispatch, job

@job("send-welcome-mail")
def send_welcome(payload, ctx):
    send_welcome_mail(payload["user_id"], timeout=ctx.time_left())

send_welcome.dispatch({"user_id": 7})                 # returns the job's id
dispatch("send-welcome-mail", {"user_id": 7}, delay=60, queue="emails")
await send_welcome.dispatch_async({"user_id": 7})     # from async code
```

A job is a name and a handler, sync or async, that takes the payload and a context. No worker process runs: the platform delivers each job back to your app. Sending one on AWS needs boto3 (`ske-bridge[jobs]`). `dispatch()` returns the job's id once the job is queued, and raises when the job isn't defined in this process, the payload isn't JSON, or the queue refuses it. Options: `delay`, in whole seconds from 0 to 900, and `queue`, the name of one of the environment's queues. On the platform `dispatch()` blocks until the queue has the message; in async code use `dispatch_async()` (also a `Job` method), which takes the same arguments and sends from a worker thread, so your event loop keeps serving. A `Job` called directly calls its handler. Defining the same function again under its name (the same module and qualified name, as when a module is imported twice) replaces the first definition; any other function under a taken name is an error.

A process knows a job only once the module defining it is imported, and the process that runs a delivered job is not always the one that dispatched it. Define jobs where every process of the app imports them:

- Django: in an app's `jobs.py`, imported from its `AppConfig.ready()`. URL configurations and views are loaded only by the first web request, which a process serving only the queue never gets.
- Flask: in a module your app (or its factory) imports when it is created.
- Any other app: in a module imported by the module `wrap()` or `queue()` names, such as `main.py`.

The handler gets the payload and a context with `id`, `name`, `attempts` (from 1), `cancelled` (a `threading.Event`, set when the job runs out of time) and `time_left()` (seconds before it does, or `None` without a limit). The payload travels as JSON, so the handler always gets a JSON copy of what was dispatched: tuples become lists, and a value JSON can't hold (a `datetime`, a `set`, `NaN`) is refused by `dispatch()`.

Where the job runs:

- **On the platform, on AWS**, `dispatch()` sends the job to the environment's SQS queue: `{SQS_PREFIX}/{name}{SQS_SUFFIX}`, where the name is `queue`, else `SQS_QUEUE`, else `default`. The names allowed are Celery's: the default queue and those `SQS_QUEUES` lists; any other is refused. The region is `AWS_REGION`, else `AWS_DEFAULT_REGION`, and the credentials are the function's role. boto3 is imported on the first dispatch.
- **On the platform, on GCP**, it creates a task in the environment's Cloud Tasks queue, as the [Celery](#celery) transport does: `{SKE_QUEUE_PREFIX}{name}` in `SKE_GCP_PROJECT` at `SKE_REGION`, where the name is `queue`, else `default`, and only names `SKE_QUEUES` lists are allowed (with none listed, none is). A delay becomes the task's schedule time. The access token comes from Cloud Run's metadata server, and is fetched again a minute before it expires; no Google SDK is needed. A refusal raises `ske_bridge.cloudtasks.CloudTasksError` with the HTTP status and the reply's body; a `429` or `5xx` is `CloudTasksUnavailable`, which is also a `URLError`.
- **Off the platform**, `dispatch()` runs the handler in your process once the delay has passed, and logs an error it raises. One background thread runs these jobs one at a time, in the order they fall due, as the platform runs a batch. The thread doesn't keep the process alive, so a script or command that exits first drops the jobs not run yet. With `SKE_QUEUE_SYNC=1` it runs the handler before `dispatch()` returns, and an error it raises is raised by `dispatch()`, which is what tests want. Either way a Flask app's jobs run in an application context and a Django project's with its database connection upkeep, as on the platform; with several Flask apps, the one set up last.

The message body is `{"job":"send-welcome-mail","payload":{"user_id":7},"id":"<uuid>"}`; the id is made once by `dispatch()` and stays the same on every delivery, so a handler can use it to do its work only once.

What becomes of a delivered message, with the queue's `tries`, `backoff` and `timeout` settings:

| The job | The message |
|---|---|
| returns | acknowledged |
| raises, with tries left | delivered again after `backoff` seconds |
| raises on its last try | kept in the queue's dead-letter queue |
| runs past its time limit | as a failure: never acknowledged, even when the handler returns |
| was not started, or was cut short by jobs before it in the batch, because the batch ran out of time | delivered again at once, with no try used up |
| names a job this app doesn't define, or isn't a job message | kept in the dead-letter queue |
| has been delivered more times than `tries` allows | kept in the dead-letter queue, without running |

`tries` counts deliveries: with `tries: 3` a job runs at most three times, whether a run failed, ran out of its time or never answered (a crash). A job released because the batch ran out of time keeps its tries: one left unstarted at the end of a full batch, or cut short because the jobs before it in the batch used up the time, is delivered again at once, rather than after `backoff`, without its failure logged and with no try used up. That relies on the platform delivering such a message again with its count kept; where it can't, and once it has done so a fixed number of times for one message, the next delivery counts as a try again. Each job may run for the queue's `timeout` (60 seconds when unset) less a margin for reporting back (5 seconds, or half the timeout when that is less), and never past the time the platform gives the whole batch, counted from when the batch's request arrives. The first job of a batch has that whole time, a little less than its own limit since the platform keeps a moment back for answering, and running out of it is the job's own timeout: it uses a try. That full time is measured from when the app gets the batch, so when the platform has just restarted the app, the restart can shorten it. A later job is cut short by the batch only when the jobs before it took more than a tenth of its time (a second at most).

When a job runs out of time its `ctx.cancelled` is set, and the bridge stops it where Python lets it:

- A sync handler on the main thread, which is how gunicorn's sync worker serves requests (and so `ske_bridge.wsgi:queue`, and Django or Flask under a sync worker), gets `JobTimeout` raised in it, through `SIGALRM`. Like `asyncio.CancelledError`, `JobTimeout` is not an `Exception`, so an `except Exception` in the handler doesn't swallow it; a handler that catches it anyway and keeps running gets it again every second.
- An async handler runs in an event loop of its own and is cancelled at its limit. It has a short grace (a second at most) to finish; one that ignores its cancellation is left behind, unfinished, and the batch goes on. On the main thread, one that holds the event loop (a blocking call) past the grace gets `JobTimeout` raised in it, as a sync handler does. Because the loop is its own, an async handler can't use connections or clients made on your app's event loop; make them inside the handler.
- A sync handler on any other thread (an ASGI app, a threaded worker) can't be interrupted: it runs until it returns. Check `ctx.cancelled` or `ctx.time_left()` between steps, and stop.

A handler that returns after its limit counts as out of time, never done, whichever way it ended: a sync handler that couldn't be interrupted, or an async one that held the event loop off the main thread. A handler that runs past the platform's own timeout makes the platform time out the whole request, and every message of the batch is delivered again, including jobs that already finished.

The jobs of a batch run one after another, each within the batch's time, so a larger batch size gives later jobs less time, and those not started or cut short in time are delivered again, keeping their tries. A job may run more than once (a redelivery after a timeout, a release), so a job whose effects must happen once checks its `id` before acting.

The bridge keeps no record of failed jobs, so every failure is kept in the queue's dead-letter queue, where you can inspect it and send it again. GCP has no dead-letter store in the platform yet: there a failed job is acknowledged after it is logged.

### Jobs and Celery on one queue

Jobs and Celery tasks can share a queue, and a batch can hold both. Each message is told apart by its body: a JSON object with a `"job"` key is a job, which Celery never sends; anything else goes to Celery. They run in the order delivered, with the batch's one deadline. A Celery message is run only when the app has a Celery app (see [Celery](#celery)); when it has none, or it can't be found, nothing of the batch runs and the queue answers `500`, so the platform delivers the batch again once the deployment is fixed. A message that is neither, in an app that defines jobs, is kept in the dead-letter queue. The queue's `tries`, `backoff` and `timeout` apply to jobs as above; Celery tasks keep Celery's own retry settings.

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

Retries and backoff are Celery's per-task settings: `max_retries`, `default_retry_delay`, `autoretry_for`, `retry_backoff`. The queue's own `tries` and `backoff` settings are not applied to Celery tasks. `tries` is the SQS queue's `maxReceiveCount`, which counts deliveries of one message; each Celery retry is a new message, so it starts that count again. Kombu's SQS transport doesn't use SQS's `DelaySeconds` for Celery messages, so a message with an eta (a countdown, a delayed `self.retry()`, an `autoretry_for` countdown) is delivered first, held unrun with a release that counts as a delivery, and run on a later one. Every task that retries with a delay therefore needs `tries` of at least 2, plus one for each 12 hours of eta: with `tries: 1` every delayed retry is kept in the dead-letter queue without running. The count also matters for a message that keeps being delivered without an outcome (a crash, a timeout of the whole invocation).

Time limits are a worker's settings: at a task's soft limit it gets `SoftTimeLimitExceeded`, and at its hard limit `TimeLimitExceeded`. A worker kills its child process at the hard limit; here `TimeLimitExceeded` is raised inside the task, so `autoretry_for` or an `except` in the task can catch it. The batch has one deadline as well, a few seconds before the platform cuts it off (the time the platform gives, or else the queue's timeout counted from arrival): each task's hard limit is cut to the time left. A task that had its full time (its own hard limit, or the batch's whole time when it has none or that is shorter) and runs out of it fails with `TimeLimitExceeded` (a task with no limit of its own is told it ran out of the batch's time). That full time is measured from when the app gets the batch, so when the platform has just restarted the app, the restart can shorten it. A task cut short because the messages before it in the batch used up the time is delivered again in a later invocation, with no failure handling run and none of the message's deliveries used up, and so is a message reached with too little time left; that relies on the platform delivering it again with its count kept, which it does a fixed number of times for one message before such deliveries count again. The bridge stops such a task with an exception `autoretry_for` doesn't retry, but a bare `except Exception` in the task still catches it, and the task then runs on past the deadline. Every other release (`Reject(requeue=True)`, a retry that couldn't be published, a task that couldn't be set up, an eta still ahead) counts as a delivery. Interrupting a task needs gunicorn's default sync worker, which runs requests on the main thread.

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
