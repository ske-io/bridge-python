"""SKE's bridge for Python applications.

It answers the platform's internal routes inside your application
(``/__ske/queue`` runs delivered Celery tasks, ``/__ske/health`` answers
readiness) and turns the Celery beat schedule into a command the platform runs
every minute. Commands themselves (``python manage.py …``, ``flask …``) need
nothing from here: the platform runs them as plain processes in the
container.

Everything is inert off-platform, so the package is safe to install
unconditionally.
"""

__version__ = "0.1.0"
