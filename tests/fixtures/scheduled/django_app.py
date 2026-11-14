"""An app whose import configures a Django project, as ``proj.wsgi`` does."""

from django.conf import settings

settings.configure()


def application(environ, start_response):
    start_response("200 OK", [])
    return [b"ok"]
