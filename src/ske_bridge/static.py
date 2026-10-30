"""Which static files a framework must not serve on the platform.

The platform serves the public directory itself and refuses dotfiles and
source files there: configuration (``.env``, ``.git``) and code are never
assets. A request it refuses goes on to the application, so a framework's own
static route must refuse the same files, or it would serve what the platform
just turned away.
"""

from __future__ import annotations

import posixpath

#: Extensions of files meant to be run, not downloaded: the platform's own list
#: and Python's bytecode and archive forms.
SOURCE_EXTENSIONS = frozenset({
    ".php", ".php3", ".php4", ".php5", ".php7", ".php8", ".phps", ".pht", ".phtml", ".phar", ".inc",
    ".cgi", ".pl", ".py", ".pyc", ".pyo", ".pyw", ".pyz", ".rb", ".sh",
})


def refused(path: str) -> bool:
    """Whether a static file path names a dotfile, a file in a dot-directory,
    or a source file."""
    segments = [s for s in path.replace("\\", "/").split("/") if s not in ("", ".")]
    if any(s.startswith(".") for s in segments):
        return True
    return posixpath.splitext(segments[-1] if segments else "")[1].lower() in SOURCE_EXTENSIONS
