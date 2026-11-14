import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from tests.conftest import FIXTURES

APP = FIXTURES / "scheduled"


def cli(*args, cwd=APP, **env):
    environment = {key: value for key, value in os.environ.items() if not key.startswith(("SKE_", "MAINTENANCE_", "CELERY_"))}
    environment.update({"SKE_SCHEDULED_AT": "2026-03-04T10:01:00Z", **env})
    return subprocess.run(
        [sys.executable, "-m", "ske_bridge.cli", *args], cwd=cwd, env=environment, capture_output=True, text=True, timeout=60,
    )


def ran(output):
    return [line.split(" ")[1] for line in output.splitlines() if line.startswith("ran ")]


ALWAYS = ["every-minute", "every-thirty-seconds", "runs-in-maintenance", "slow", "fails-when-asked", "dispatches"]


def test_the_cli_imports_the_app_and_runs_the_tasks_due():
    done = cli("schedule", "--app", "main:app", SKE_SCHEDULED_AT="2026-03-04T10:15:42Z")

    assert done.returncode == 0, done.stderr
    assert ran(done.stdout) == ["every-minute", "quarter-past", *ALWAYS[1:]]
    assert "slow finished" in done.stdout


def test_the_cli_takes_the_app_from_ske_app():
    done = cli("schedule", SKE_APP="main:app", SKE_SCHEDULED_AT="2026-01-15T08:00:00Z")

    assert done.returncode == 0, done.stderr
    assert "paris-nine" in ran(done.stdout) and "hourly" in ran(done.stdout)


def test_the_cli_flag_wins_over_ske_app():
    done = cli("schedule", "--app", "main:app", SKE_APP="broken:app")

    assert done.returncode == 0, done.stderr


def test_the_cli_exits_non_zero_when_a_task_fails():
    done = cli("schedule", "--app", "main:app", FAIL_TASK="1")

    assert done.returncode == 1
    assert "failed fails-when-asked: RuntimeError: the task broke" in done.stdout
    assert "ran slow" in done.stdout
    assert "1 scheduled task(s) failed: fails-when-asked" in done.stdout


@pytest.mark.parametrize(("target", "message"), [
    ("broken:app", "the app is broken"),
    ("missing:app", "No module named 'missing'"),
    ("main:nope", "has no attribute 'nope'"),
    ("main", "not an import string"),
])
def test_the_cli_exits_non_zero_when_the_app_cannot_be_imported(target, message):
    done = cli("schedule", "--app", target)

    assert done.returncode == 1
    assert f"the app {target} could not be imported" in done.stderr
    assert message in done.stderr


@pytest.mark.parametrize(("target", "command"), [
    ("flask_app:app", "flask ske schedule"),
    ("django_app:application", "python manage.py ske_schedule"),
])
def test_the_cli_refuses_a_flask_app_or_a_django_project(target, command):
    done = cli("schedule", "--app", target)

    assert done.returncode == 2
    assert f"run its schedule with `{command}`" in done.stderr


def test_the_cli_puts_the_current_directory_first_on_sys_path():
    # A console script's sys.path starts with its own directory, not the
    # current one; here the current one is on it, but behind a directory
    # with a module of the same name, which would win.
    script = Path(sys.executable).parent / "ske-bridge"
    environment = {key: value for key, value in os.environ.items() if not key.startswith(("SKE_", "MAINTENANCE_", "CELERY_"))}
    environment.update({"SKE_SCHEDULED_AT": "2026-03-04T10:01:00Z", "PYTHONPATH": os.pathsep.join([str(FIXTURES / "scheduled_decoy"), str(APP)])})

    done = subprocess.run([str(script), "schedule", "--app", "main:app"], cwd=APP, env=environment, capture_output=True, text=True, timeout=60)

    assert done.returncode == 0, done.stderr
    assert "ran every-minute" in done.stdout


def test_without_an_app_or_a_celery_app_the_cli_has_nothing_to_run():
    done = cli("schedule")

    assert done.returncode == 0
    assert "no app named" in done.stdout


def test_the_cli_refuses_unknown_arguments():
    assert cli().returncode == 2
    assert cli("run").returncode == 2
    assert cli("schedule", "extra").returncode == 2


def test_the_cli_on_the_platform_skips_tasks_in_maintenance():
    done = cli("schedule", "--app", "main:app", SKE_ENVIRONMENT="production", MAINTENANCE_MODE="true")

    assert done.returncode == 0, done.stderr
    assert ran(done.stdout) == ["runs-in-maintenance"]
    assert "skipped every-minute: maintenance mode is on" in done.stdout


def test_the_cli_on_the_platform_logs_as_the_platform_reads_logs():
    done = cli("schedule", "--app", "main:app", SKE_ENVIRONMENT="production", FAIL_TASK="1")

    assert done.returncode == 1
    error, warning = [json.loads(line) for line in done.stderr.splitlines()]
    assert warning["level_name"] == "WARNING" and "every-thirty-seconds" in warning["message"]
    assert error["message"] == "ske: scheduled task fails-when-asked failed"
    assert error["context"]["exception"]["message"] == "the task broke"


def test_off_the_platform_the_cli_waits_for_the_jobs_its_tasks_dispatched():
    done = cli("schedule", "--app", "main:app", DISPATCH="1", DELAYED_JOB="1")

    assert done.returncode == 0, done.stderr
    assert sorted(line for line in done.stdout.splitlines() if "ran" in line and not line.startswith("ran ")) == [
        "child ran 1", "child ran 2", "parent ran",
    ]


def test_off_the_platform_a_failing_job_fails_the_cli():
    done = cli("schedule", "--app", "main:app", DISPATCH="1", FAIL_JOB="1")

    assert done.returncode == 1
    assert "child ran 1" in done.stdout
    assert "the job broke" in done.stderr
    assert "1 job(s) dispatched by the schedule did not finish" in done.stdout


def test_off_the_platform_a_job_due_after_the_wait_fails_the_cli_at_once():
    started = time.monotonic()
    done = cli("schedule", "--app", "main:app", LATE_JOB="1")

    assert time.monotonic() - started < 30
    assert done.returncode == 1
    assert "did not finish" in done.stdout


def test_the_console_script_is_installed():
    script = Path(sys.executable).parent / "ske-bridge"

    done = subprocess.run([str(script), "schedule", "--help"], capture_output=True, text=True, timeout=60)

    assert done.returncode == 0
    assert "--app MODULE:ATTRIBUTE" in done.stdout
