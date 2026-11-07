from ske_bridge import cache


def test_the_prefix_comes_from_the_environment(monkeypatch):
    monkeypatch.setenv("SKE_CACHE_PREFIX", "shop-pr-1:cache:")

    assert cache.prefix() == "shop-pr-1:cache:"


def test_an_unset_or_empty_prefix_is_none(monkeypatch):
    monkeypatch.delenv("SKE_CACHE_PREFIX", raising=False)
    assert cache.prefix() is None

    monkeypatch.setenv("SKE_CACHE_PREFIX", "")
    assert cache.prefix() is None


def test_the_platform_prefix_goes_before_the_apps_own():
    assert cache.prefixed("myapp", "shop-pr-1:cache:") == "shop-pr-1:cache:myapp"


def test_no_own_prefix_leaves_the_platform_prefix():
    assert cache.prefixed(None, "shop-pr-1:cache:") == "shop-pr-1:cache:"
    assert cache.prefixed("", "shop-pr-1:cache:") == "shop-pr-1:cache:"


def test_applying_the_prefix_twice_changes_nothing():
    once = cache.prefixed("myapp", "shop-pr-1:cache:")

    assert cache.prefixed(once, "shop-pr-1:cache:") == once


def test_a_callable_prefix_stays_lazy_behind_the_platform_prefix():
    calls = []

    def own():
        calls.append(1)
        return "tenant:"

    wrapped = cache.prefixed(own, "shop-pr-1:cache:")

    assert calls == []
    assert wrapped() == "shop-pr-1:cache:tenant:"


def test_wrapping_a_callable_prefix_twice_changes_nothing():
    wrapped = cache.prefixed(lambda: "tenant:", "shop-pr-1:cache:")

    assert cache.prefixed(wrapped, "shop-pr-1:cache:") is wrapped

