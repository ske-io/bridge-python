"""Maintenance mode, driven by the platform.

The platform puts an environment into maintenance by redeploying it with
``MAINTENANCE_MODE=true``, so every instance sees the same state; a command
run in one container could not reach the others, and the next deploy would
undo it. While it is on, web requests are answered ``503 Service
Unavailable`` with your app's ``503.html`` template, or a minimal page when it
has none.

What keeps working:

- The platform's own requests: the readiness route, delivered queue messages
  (``/__ske/*``, answered before this check) and anything else the platform
  sends with its internal header, such as its warmup requests.
- Requests whose ``SKE-Maintenance-Bypass`` header equals
  ``SKE_MAINTENANCE_BYPASS_TOKEN``, a secret the platform puts in the
  environment when the instance starts. The platform sets this header on
  requests from addresses on the environment's access list, after removing
  any copy a client sent. These requests reach the app normally without
  setting a bypass cookie. Without a token the header is ignored, so a
  client's copy passed along by a platform that sets none opens nothing.
- Whoever has the bypass secret. When ``MAINTENANCE_MODE_SECRET`` is set,
  visiting ``/<secret>`` sets a cookie and redirects to ``/``; the cookie
  lets that browser in for 12 hours. It is signed with the secret, so it
  stops working when the secret changes, and cannot be made without it.

The scheduler sends no beat entries while maintenance is on, except entries
marked to run anyway; see ske_bridge.celery.scheduler.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from dataclasses import dataclass
from http.cookies import CookieError, SimpleCookie

from ske_bridge import environment

COOKIE = "ske_maintenance"
BYPASS_HEADER = "SKE-Maintenance-Bypass"
BYPASS_TOKEN = "SKE_MAINTENANCE_BYPASS_TOKEN"

#: How long the bypass cookie lets a browser in, in seconds.
LIFETIME = 12 * 60 * 60

#: The page answered when the app has no ``503.html`` template.
DEFAULT_PAGE = (
    b"<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">"
    b"<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
    b"<title>Service Unavailable</title></head>"
    b"<body><h1>Service Unavailable</h1>"
    b"<p>This site is down for maintenance. Please try again shortly.</p></body></html>"
)

TEMPLATE = "503.html"


@dataclass(frozen=True)
class Bypass:
    """Let the visitor in from now on: redirect to ``/`` with this cookie."""

    cookie: str


@dataclass(frozen=True)
class Paused:
    """Answer 503 with the maintenance page."""


def active() -> bool:
    """Whether the platform has this environment in maintenance."""
    return environment.running_on_platform() and environment.get("MAINTENANCE_MODE") == "true"


def bypassed_by_platform(header: str | None) -> bool:
    """Whether a ``SKE-Maintenance-Bypass`` value is the platform's token.
    Never true without a token, compared in constant time."""
    token = environment.get(BYPASS_TOKEN)
    if token is None or header is None:
        return False
    return hmac.compare_digest(token.encode("utf-8", "surrogatepass"), header.encode("utf-8", "surrogatepass"))


def check(
    path: str, cookie_header: str | None, secure: bool, internal: bool = False,
    *, bypass_header: str | None = None,
) -> Bypass | Paused | None:
    """What maintenance mode does with a web request: nothing (None), let it
    in with a new bypass cookie, or answer it with the maintenance page.

    ``internal`` is whether the request carries the platform's internal
    header, which the container's entry process strips from outside traffic.
    ``bypass_header`` is the request's ``SKE-Maintenance-Bypass`` value. The
    platform sets it to its token on requests from addresses on the
    environment's access list, after removing any copy a client sent.
    """
    if internal or not active() or bypassed_by_platform(bypass_header):
        return None

    secret = environment.get("MAINTENANCE_MODE_SECRET")
    if secret is not None:
        if hmac.compare_digest(path.strip("/").encode(), secret.encode()):
            return Bypass(_bypass_cookie(secret, secure))
        if _has_valid_cookie(cookie_header, secret):
            return None

    return Paused()


def _bypass_cookie(secret: str, secure: bool) -> str:
    expires = int(time.time()) + LIFETIME
    value = base64.urlsafe_b64encode(json.dumps({"expires_at": expires, "mac": _mac(secret, expires)}).encode()).decode()

    cookie = SimpleCookie()
    cookie[COOKIE] = value
    morsel = cookie[COOKIE]
    morsel["path"] = "/"
    morsel["max-age"] = str(LIFETIME)
    morsel["httponly"] = True
    morsel["samesite"] = "Lax"
    if secure:
        morsel["secure"] = True
    return morsel.OutputString()


def _has_valid_cookie(cookie_header: str | None, secret: str) -> bool:
    if not cookie_header:
        return False
    try:
        cookies = SimpleCookie(cookie_header)
        if COOKIE not in cookies:
            return False
        payload = json.loads(base64.urlsafe_b64decode(cookies[COOKIE].value.encode()))
        expires = int(payload["expires_at"])
        mac = str(payload["mac"])
    except (CookieError, ValueError, TypeError, KeyError):
        return False
    return hmac.compare_digest(mac, _mac(secret, expires)) and time.time() < expires


def _mac(secret: str, expires: int) -> str:
    return hmac.new(secret.encode(), str(expires).encode(), hashlib.sha256).hexdigest()
