"""What the platform says about the client, in the headers it forwards.

The application listens on a loopback address behind the container's entry
process, so the connection it sees always comes from 127.0.0.1. The entry
process forwards every request with headers it sets itself:

- ``X-Forwarded-For``: whatever chain the client sent, with the address the
  platform saw the request come from appended as the last entry. Only that
  last entry is vouched for; everything before it is the client's own claim.
- ``X-Forwarded-Proto``, ``X-Forwarded-Host`` and ``X-Forwarded-Port``:
  replaced on every request with the scheme, host and port the client
  connected with, so a value the client sent never reaches the application.
- ``Forwarded``, ``X-Real-IP``, ``X-Forwarded-Prefix`` and the like are
  removed.

So exactly one proxy hop is trusted, and only on the platform: off it, the
same headers would be whatever a client chose to send.
"""

from __future__ import annotations

import ipaddress
from dataclasses import dataclass

#: Proxies in front of the application whose X-Forwarded-* entries are trusted.
TRUSTED_HOPS = 1


def client_address(forwarded_for: str | None) -> str | None:
    """The client's address from an ``X-Forwarded-For`` value: its last
    entry, the one the platform appended. None when there is no header or the
    entry is not an IP address, which leaves the connection's own address in
    place."""
    if not forwarded_for:
        return None
    last = forwarded_for.rsplit(",", 1)[-1].strip()
    try:
        return str(ipaddress.ip_address(last))
    except ValueError:
        return None


#: The port each scheme is reached on when the Host header names none.
DEFAULT_PORTS = {"http": 80, "https": 443}


@dataclass(frozen=True)
class Origin:
    """What the trusted hop says about a request, for the server-neutral
    middleware (ske_bridge.wsgi.TrustedProxy and ske_bridge.asgi.TrustedProxy)
    to put where an application reads it. Each field is None when the headers
    say nothing usable about it, which leaves the connection's own value."""

    #: The client's address.
    client: str | None = None
    #: ``http`` or ``https``.
    scheme: str | None = None
    #: The Host header value: the host, with the port unless it is the
    #: scheme's default.
    host: str | None = None
    #: The host's name and port, as a server reports them: an IPv6 address
    #: without its brackets.
    server: tuple[str, int] | None = None


def origin(
    forwarded_for: str | None, proto: str | None, host: str | None, port: str | None, scheme: str,
) -> Origin:
    """The client, scheme and host from one trusted hop's ``X-Forwarded-For``,
    ``-Proto``, ``-Host`` and ``-Port`` values: the last entry of each.
    ``scheme`` is the connection's own, used when ``X-Forwarded-Proto`` says
    nothing. ``X-Forwarded-Port`` replaces any port ``X-Forwarded-Host``
    names; without ``X-Forwarded-Host`` the host is left alone."""
    forwarded_scheme = _last(proto)
    forwarded_scheme = forwarded_scheme.lower() if forwarded_scheme else None
    if forwarded_scheme not in DEFAULT_PORTS:
        forwarded_scheme = None

    effective_scheme = forwarded_scheme or scheme
    forwarded_host = _last(host)
    if forwarded_host is None:
        return Origin(client_address(forwarded_for), forwarded_scheme)

    name, own_port = _split_host(forwarded_host)
    effective_port = _port(_last(port)) or own_port or DEFAULT_PORTS.get(effective_scheme)
    if effective_port is None or DEFAULT_PORTS.get(effective_scheme) == effective_port:
        value = name
    else:
        value = f"{name}:{effective_port}"
    bare = name[1:-1] if name.startswith("[") and name.endswith("]") else name
    server = (bare, effective_port) if effective_port is not None else None
    return Origin(client_address(forwarded_for), forwarded_scheme, value, server)


def _last(value: str | None) -> str | None:
    if not value:
        return None
    last = value.rsplit(",", 1)[-1].strip()
    return last or None


def _port(value: str | None) -> int | None:
    if value is None or not (value.isascii() and value.isdigit()):
        return None
    port = int(value)
    return port if 0 < port < 65536 else None


def _split_host(host: str) -> tuple[str, int | None]:
    """``example.com:8080`` into its name and port; an IPv6 literal keeps its
    brackets."""
    if host.endswith("]") or ":" not in host:
        return host, None
    name, _, port = host.rpartition(":")
    parsed = _port(port)
    return (name, parsed) if parsed is not None else (host, None)
