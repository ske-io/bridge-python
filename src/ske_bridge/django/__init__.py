"""The Django integration.

Add the app and the database helper to settings.py::

    import ske_bridge.django

    INSTALLED_APPS = [..., "ske_bridge.django"]
    DATABASES = ske_bridge.django.database(default="sqlite:///db.sqlite3")

On the platform the app puts its middleware at the front of ``MIDDLEWARE``
(see apps.py), so the platform's internal routes need no change to urls.py,
and adds the ``ske_schedule`` management command the scheduler runs.

This module is imported from settings.py, before Django is set up, so it
imports nothing from Django.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import parse_qsl, unquote, urlsplit

from ske_bridge import environment

ENGINES = {
    "postgres": "django.db.backends.postgresql",
    "postgresql": "django.db.backends.postgresql",
    "pgsql": "django.db.backends.postgresql",
    "mysql": "django.db.backends.mysql",
    "mysql2": "django.db.backends.mysql",
    "mariadb": "django.db.backends.mysql",
    "sqlite": "django.db.backends.sqlite3",
    "sqlite3": "django.db.backends.sqlite3",
}


def database(
    url: str | None = None,
    *,
    default: str | None = None,
    conn_max_age: int | None = 0,
    conn_health_checks: bool = False,
) -> dict[str, dict[str, Any]]:
    """``DATABASES`` from ``DATABASE_URL``, which the platform sets for an
    environment's database.

    ``url`` overrides the variable and ``default`` is used when it is unset,
    for local development. With neither, the result is ``{}``: Django's
    setting for an application without a database, which is also what lets
    ``collectstatic`` import settings at build time, when no database
    exists.

    The URL is ``scheme://user:password@host:port/name?option=value``, for
    PostgreSQL (``postgres``, ``postgresql``), MySQL or MariaDB (``mysql``,
    ``mariadb``) and SQLite (``sqlite:///relative.db``,
    ``sqlite:////absolute.db``). A driver suffix (``postgresql+psycopg``) is
    ignored. User, password, host and name are percent-decoded, so a unix
    socket path goes in the host as ``%2Fcloudsql%2Fproject:region:instance``.
    Query options go to ``OPTIONS`` (``sslmode=require``), whole numbers as
    integers.
    """
    url = url or environment.get("DATABASE_URL") or default
    if url is None:
        return {}
    return {"default": parse(url, conn_max_age=conn_max_age, conn_health_checks=conn_health_checks)}


def parse(url: str, *, conn_max_age: int | None = 0, conn_health_checks: bool = False) -> dict[str, Any]:
    """One ``DATABASES`` entry from a database URL; see database()."""
    parts = urlsplit(url)
    if parts.fragment or ("@" not in parts.netloc and "@" in parts.path + parts.query):
        # urlsplit ends the credentials at the first "/", "?" or "#", so an
        # unencoded one in the password puts the rest of the URL in the wrong
        # place, and the host it reads is part of the password.
        raise ValueError(
            "the database URL cannot be read: percent-encode '/', '?' and '#' in the user and password "
            "(%2F, %3F and %23)"
        )
    scheme = parts.scheme.split("+", 1)[0].lower()
    engine = ENGINES.get(scheme)
    if engine is None:
        raise ValueError(f"unsupported database URL scheme {parts.scheme!r}; use one of {', '.join(sorted(ENGINES))}")

    options: dict[str, Any] = {k: int(v) if v.isascii() and v.isdigit() else v for k, v in parse_qsl(parts.query)}

    if engine == ENGINES["sqlite"]:
        # sqlite:///db.sqlite3 is relative, sqlite:////var/db.sqlite3
        # absolute, and sqlite://:memory: in memory.
        name = unquote(parts.path[1:] if parts.path.startswith("/") else parts.path) or unquote(parts.netloc)
        config: dict[str, Any] = {"ENGINE": engine, "NAME": name or ":memory:"}
        if options:
            config["OPTIONS"] = options
        return config

    userinfo, _, hostport = parts.netloc.rpartition("@")
    user, _, password = userinfo.partition(":")
    host, port = _host_port(hostport)

    config = {
        "ENGINE": engine,
        "NAME": unquote(parts.path.lstrip("/")),
        "USER": unquote(user),
        "PASSWORD": unquote(password),
        "HOST": host,
        "PORT": port,
        "CONN_MAX_AGE": conn_max_age,
        "CONN_HEALTH_CHECKS": conn_health_checks,
    }
    if options:
        config["OPTIONS"] = options
    return config


def _host_port(hostport: str) -> tuple[str, str]:
    """Host and port, read by hand: ``urlsplit`` would read the colons of a
    percent-encoded socket path as a port."""
    if hostport.startswith("["):
        host, _, rest = hostport[1:].partition("]")
        return host, rest.removeprefix(":")
    if hostport.lower().startswith("%2f"):
        return unquote(hostport), ""
    host, sep, port = hostport.rpartition(":")
    if not sep or not (port.isascii() and port.isdigit()):
        return unquote(hostport), ""
    return unquote(host), port
