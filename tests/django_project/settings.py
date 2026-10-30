"""A minimal Django project with the bridge installed, as an application sets it up."""

import ske_bridge.django

SECRET_KEY = "test-only"
DEBUG = False
# Not the test client's host: the platform's requests must not need it.
ALLOWED_HOSTS = ["app.example.com"]
ROOT_URLCONF = "tests.django_project.urls"
INSTALLED_APPS = ["django.contrib.contenttypes", "django.contrib.auth", "ske_bridge.django"]
MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
]
DATABASES = ske_bridge.django.database(default="sqlite://:memory:")
# Smaller than one queue batch, which the bridge must still read whole.
DATA_UPLOAD_MAX_MEMORY_SIZE = 1024
USE_TZ = True

TEMPLATES = [{
    "BACKEND": "django.template.backends.django.DjangoTemplates",
    "DIRS": [__import__("pathlib").Path(__file__).parent / "templates"],
}]
