"""The platform's mail service.

When a mail service is attached to the environment, the platform runs an SMTP
server inside the container and sets ``MAIL_HOST`` and ``MAIL_PORT`` to it.
It catches every mail the application sends, so nothing reaches a real
inbox. It speaks plain SMTP: it offers neither STARTTLS nor AUTH, so a client
must connect without TLS and must not log in (Python's ``smtplib`` refuses to
log in to a server that does not offer AUTH).

The framework integrations point the application's SMTP settings at it, on
the platform only, and override settings the project made for its own SMTP
server: the service is attached to catch mail on purpose, and a project's
real server would let mail out.
"""

from __future__ import annotations

import logging

from ske_bridge import environment

logger = logging.getLogger("ske_bridge")


def mail_service() -> tuple[str, int] | None:
    """``(host, port)`` of the platform's mail service, or None when none is
    attached or this is not the platform."""
    if not environment.running_on_platform():
        return None

    host = environment.get("MAIL_HOST")
    port = environment.get("MAIL_PORT")
    if host is None or port is None:
        return None
    if not (port.isascii() and port.isdigit()):
        logger.warning("ske: MAIL_PORT %r is not a port; mail settings are left as they are", port)
        return None
    return host, int(port)
