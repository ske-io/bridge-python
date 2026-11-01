# SKE Python bridge

Python package (`ske-bridge`, imported as `ske_bridge`) installed in tenant Django and Flask apps. It handles the platform's `/__ske/queue` and `/__ske/health` routes, runs Celery tasks from delivered messages, and exposes the beat schedule as the scheduler command. It supports Python 3.11–3.14, Django 4.2+, Flask 3.0+ and Celery 5.3+; Django, Flask and Celery are optional extras, imported only by the module that needs them.

Layout: `routes.py` is the framework-agnostic route handling, `django/` and `flask.py` wire it into each framework, and `celery/` holds message decoding, the queue runner, outcomes and the scheduler.

## Check

Done means `uv run pytest` passes (also on the oldest Python: `uv run --python 3.11 --isolated --with-editable . --group dev pytest`).

Commit subjects are plain sentences.

## Releasing

Published on PyPI as `ske-bridge` by `.github/workflows/publish.yml` through trusted publishing (no token): a manual run uploads to TestPyPI (`testpypi` environment), a published GitHub release uploads to PyPI (`pypi` environment, which needs approval). To release, bump `version` in `pyproject.toml`, merge, then `gh release create vX.Y.Z --generate-notes`; the workflow stops if the tag is not `v` + that version. PyPI never takes a version twice: yank a bad release and ship a new version.
