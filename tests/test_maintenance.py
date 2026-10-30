import base64
import json
import time

import pytest

from ske_bridge import maintenance
from ske_bridge.maintenance import COOKIE, LIFETIME, Bypass, Paused, check


@pytest.fixture
def down(platform, monkeypatch):
    monkeypatch.setenv("MAINTENANCE_MODE", "true")
    monkeypatch.setenv("MAINTENANCE_MODE_SECRET", "preview-secret")


def cookie_from(decision):
    return decision.cookie.split(";", 1)[0]


def test_web_requests_are_paused(down):
    assert isinstance(check("/", None, True), Paused)
    assert isinstance(check("/orders/7", None, True), Paused)


def test_the_platforms_own_requests_pass(down):
    assert check("/warm", None, True, internal=True) is None


@pytest.mark.parametrize("path", ["/preview-secret", "/preview-secret/", "preview-secret"])
def test_the_secret_path_bypasses_with_a_cookie(down, path):
    decision = check(path, None, True)

    assert isinstance(decision, Bypass)
    assert decision.cookie.startswith(f"{COOKIE}=")
    for attribute in ("Path=/", f"Max-Age={LIFETIME}", "HttpOnly", "SameSite=Lax", "Secure"):
        assert attribute in decision.cookie


def test_the_cookie_is_secure_only_over_https(down):
    assert "Secure" not in check("/preview-secret", None, False).cookie


def test_the_bypass_cookie_lets_the_visitor_in(down):
    cookie = cookie_from(check("/preview-secret", None, True))

    assert check("/", cookie, True) is None
    assert check("/orders/7", f"theme=dark; {cookie}", True) is None


def test_a_forged_cookie_does_not(down):
    forged = base64.urlsafe_b64encode(json.dumps({"expires_at": int(time.time()) + 100, "mac": "0" * 64}).encode()).decode()

    assert isinstance(check("/", f"{COOKIE}={forged}", True), Paused)
    assert isinstance(check("/", f"{COOKIE}=garbage", True), Paused)


def test_the_cookie_expires(down, monkeypatch):
    monkeypatch.setattr(time, "time", lambda: 1_000_000)
    cookie = cookie_from(check("/preview-secret", None, True))
    monkeypatch.setattr(time, "time", lambda: 1_000_000 + LIFETIME + 1)

    assert isinstance(check("/", cookie, True), Paused)


def test_a_new_secret_ends_old_cookies(down, monkeypatch):
    cookie = cookie_from(check("/preview-secret", None, True))
    monkeypatch.setenv("MAINTENANCE_MODE_SECRET", "rotated")

    assert isinstance(check("/", cookie, True), Paused)


def test_without_a_secret_nothing_bypasses(down, monkeypatch):
    monkeypatch.delenv("MAINTENANCE_MODE_SECRET")

    assert isinstance(check("/preview-secret", None, True), Paused)


@pytest.mark.parametrize("value", ["false", "1", ""])
def test_only_true_means_maintenance(platform, monkeypatch, value):
    monkeypatch.setenv("MAINTENANCE_MODE", value)

    assert check("/", None, True) is None


def test_off_the_platform_nothing_is_paused(monkeypatch):
    monkeypatch.setenv("MAINTENANCE_MODE", "true")

    assert not maintenance.active()
    assert check("/", None, True) is None
