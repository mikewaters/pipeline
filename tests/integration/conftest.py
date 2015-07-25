"""
conftest.py

py.test hook file
"""

# Note the lack of pytest_configure, it is on purpose.
# The import of tasks in tests/integration/__init__.py (
# which is required for the worker to discover the testing tasks)
# causes a recursion in pytest_configure.
# Celery config for the integration tests can be found in
# integration/__init__.py, so it is discoverable by the worker.
# To start the integration tests worker, run
# > cd tests
# > celery -A integration worker -l debug



def pytest_addoption(parser):
    parser.addoption(
        "--broker", action="store_true",
        help="run with broker"
    )
