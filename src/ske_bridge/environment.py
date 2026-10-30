"""Reads the variables the platform sets.

A gunicorn worker, a management command and a scheduled run all inherit the
process environment, and ``os.environ`` is that environment, so nothing has
to be copied anywhere before the framework boots. Dotenv loaders
(python-dotenv, django-environ) leave a variable that is already set alone
unless told to override it, so a stale ``.env`` baked into the image cannot
replace what the platform set either.
"""

import os


def get(key: str) -> str | None:
    """One variable, or None when it is unset or empty.

    An empty value is treated as absent: an unset Lambda environment variable
    and one set to "" mean the same thing to a deployment.
    """
    value = os.environ.get(key)
    return value if value else None


def running_on_platform() -> bool:
    """Whether this process runs in an SKE deployment rather than on a
    developer's machine. The platform sets both variables on every function."""
    return get("SKE_PROJECT_TYPE") is not None or get("SKE_ENVIRONMENT") is not None
