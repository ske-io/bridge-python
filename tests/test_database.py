import pytest

from ske_bridge.django import database, parse


def test_postgres_url(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://ske:p%40ss%3Aword@db.internal.example.com:5432/shop?sslmode=require&connect_timeout=5")

    assert database() == {"default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": "shop",
        "USER": "ske",
        "PASSWORD": "p@ss:word",
        "HOST": "db.internal.example.com",
        "PORT": "5432",
        "CONN_MAX_AGE": 0,
        "CONN_HEALTH_CHECKS": False,
        "OPTIONS": {"sslmode": "require", "connect_timeout": 5},
    }}


@pytest.mark.parametrize("scheme", ["postgres", "pgsql", "postgresql+psycopg"])
def test_postgres_aliases(scheme):
    assert parse(f"{scheme}://u:p@h/db")["ENGINE"] == "django.db.backends.postgresql"


@pytest.mark.parametrize("scheme", ["mysql", "mysql2", "mariadb"])
def test_mysql(scheme):
    config = parse(f"{scheme}://u:p@mysql.example.com:3306/shop?charset=utf8mb4")

    assert config["ENGINE"] == "django.db.backends.mysql"
    assert (config["HOST"], config["PORT"], config["NAME"]) == ("mysql.example.com", "3306", "shop")
    assert config["OPTIONS"] == {"charset": "utf8mb4"}


def test_a_socket_path_host():
    config = parse("postgresql://u:p@%2Fcloudsql%2Fproject%3Aregion%3Ainstance/shop")

    assert (config["HOST"], config["PORT"]) == ("/cloudsql/project:region:instance", "")


def test_an_ipv6_host():
    config = parse("postgresql://u:p@[::1]:6432/shop")

    assert (config["HOST"], config["PORT"]) == ("::1", "6432")


def test_no_port_and_no_password():
    config = parse("postgresql://u@h/shop")

    assert (config["HOST"], config["PORT"], config["PASSWORD"]) == ("h", "", "")


@pytest.mark.parametrize(("url", "name"), [
    ("sqlite:///db.sqlite3", "db.sqlite3"),
    ("sqlite:////var/data/db.sqlite3", "/var/data/db.sqlite3"),
    ("sqlite://:memory:", ":memory:"),
    ("sqlite://", ":memory:"),
])
def test_sqlite(url, name):
    assert parse(url) == {"ENGINE": "django.db.backends.sqlite3", "NAME": name}


def test_connection_settings_pass_through():
    config = database("postgresql://u:p@h/db", conn_max_age=60, conn_health_checks=True)["default"]

    assert (config["CONN_MAX_AGE"], config["CONN_HEALTH_CHECKS"]) == (60, True)


def test_the_default_applies_when_the_variable_is_unset_or_empty(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "")

    assert database(default="sqlite:///local.db")["default"]["NAME"] == "local.db"


def test_no_url_means_no_database(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)

    assert database() == {}


def test_an_unknown_scheme_is_refused():
    with pytest.raises(ValueError, match="unsupported database URL scheme 'oracle'"):
        parse("oracle://u:p@h/db")


def test_non_ascii_digits_stay_strings():
    assert parse("postgresql://u:p@h/db?connect_timeout=\u0663")["OPTIONS"] == {"connect_timeout": "\u0663"}


@pytest.mark.parametrize("password", ["pa#ss", "pa?ss", "pa/ss"])
def test_an_unencoded_separator_in_the_password_is_refused(password):
    with pytest.raises(ValueError, match="percent-encode"):
        parse(f"postgresql://u:{password}@db.example.com:5432/shop")


def test_an_encoded_separator_in_the_password_is_read():
    assert parse("postgresql://u:pa%23%3F%2Fss@db.example.com/shop")["PASSWORD"] == "pa#?/ss"


def test_a_plus_in_the_password_is_kept():
    assert parse("postgresql://u:pa+ss@db.example.com/shop")["PASSWORD"] == "pa+ss"
