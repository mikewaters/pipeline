"""
conftest.py

py.test hook file
"""
import pytest

# Note the lack of pytest_configure, it is on purpose.
# The import of tasks in tests/integration/__init__.py (
# which is required for the worker to discover the testing tasks)
# causes a recursion in pytest_configure.
# Celery config for the integration tests can be found in
# integration/__init__.py, so it is discoverable by the worker.
# To start the integration tests worker, run the following.
# Note that you must unset DJNGO_SETTINGS_MODULE b/c celery has
# some builtin django support that will break the worker.
# > cd tests
# > unset DJANGO_SETTINGS_MODULE
# > celery -A integration worker -l debug

@pytest.fixture
def scoped_broker():
    """Fixture that privides a locally-scoped Celery broker."""
    from celery import Celery

    class Config:
        BROKER_URL = ''  #'redis://localhost:6380/0'
        CELERY_RESULT_BACKEND = '' # 'redis://localhost:6380/0'
        CELERY_ALWAYS_EAGER = True

    app = Celery()
    app.config_from_object(Config)
    return app

def pytest_addoption(parser):
    parser.addoption(
        "--broker", action="store_true",
        help="run with broker"
    )
