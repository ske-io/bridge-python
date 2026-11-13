# SKE Python bridge

Python package (`ske-bridge`, imported as `ske_bridge`) installed in Django and Flask apps. It handles the platform's `/__ske/queue` and `/__ske/health` routes, runs its own jobs and Celery tasks from delivered messages, and exposes the beat schedule as the scheduler command. It supports Python 3.11–3.14, Django 4.2+, Flask 3.0+ and Celery 5.3+; Django, Flask, Celery and boto3 (the `jobs` extra, for sending jobs on AWS) are optional extras, imported only by the module that needs them.

Layout: `routes.py` is the framework-agnostic route handling, `django/` and `flask.py` wire it into each framework, `wsgi.py` and `asgi.py` hold the middleware and the `wrap()` entry points for any other WSGI or ASGI app (`generic.py`: import strings, the queue runner), `queue.py` runs a delivered batch (job messages and Celery messages, one deadline, `outcome.py`'s outcomes), `jobs/` holds the jobs API, its message, sending, local scheduler and runner, `cloudtasks.py` creates Cloud Tasks tasks for both jobs and Celery, and `celery/` holds Celery message decoding, its runner and the scheduler.

## Check

Done means `uv run pytest` passes (also on the oldest Python: `uv run --python 3.11 --isolated --with-editable . --group dev pytest`).

Commit subjects are plain sentences.

## Releasing

Published on PyPI as `ske-bridge` by `.github/workflows/publish.yml` through trusted publishing (no token): a manual run uploads to TestPyPI (`testpypi` environment), a published GitHub release uploads to PyPI (`pypi` environment, which needs approval). To release, bump `version` in `pyproject.toml`, merge, then `gh release create vX.Y.Z --generate-notes`; the workflow stops if the tag is not `v` + that version. PyPI never takes a version twice: yank a bad release and ship a new version.
