"""Keeps a preview environment's cache keys apart from its base environment's.

A preview shares its base environment's cache server, so the platform sets
``SKE_CACHE_PREFIX`` on each preview, for example ``shop-pr-1:cache:``. Django
and Flask read no variable for a key prefix, so the integrations apply it to
the app's caches: every ``CACHES`` alias's ``KEY_PREFIX`` in Django, and
Flask-Caching's ``CACHE_KEY_PREFIX``. The app's own prefix is kept after it.
"""

from __future__ import annotations

from typing import Callable, Union

from ske_bridge import environment

VARIABLE = "SKE_CACHE_PREFIX"

Prefix = Union[str, Callable[[], str], None]

# The attribute that marks a callable prefix as already wrapped, and with
# which platform prefix.
_WRAPPED = "ske_cache_prefix"


def prefix() -> str | None:
    """The platform's cache key prefix, or None when it set none."""
    return environment.get(VARIABLE)


def prefixed(own: Prefix, platform: str) -> Prefix:
    """The platform's prefix followed by the app's own.

    A callable prefix (Redis backends evaluate one for each key) is wrapped,
    so it stays lazy and its result gets the platform's prefix. Applying the
    platform's prefix again changes nothing: a string that already starts
    with it, or a callable already wrapped with it, is returned as it is."""
    if callable(own):
        if getattr(own, _WRAPPED, None) == platform:
            return own

        def wrapped() -> str:
            return prefixed(own(), platform)

        setattr(wrapped, _WRAPPED, platform)
        return wrapped

    own = own or ""
    return own if own.startswith(platform) else platform + own
