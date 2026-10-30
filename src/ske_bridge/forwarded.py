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
