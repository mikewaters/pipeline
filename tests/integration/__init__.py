"""
See conftest.py for info on how to run the worker.

"""
import os
from celery import Celery

# Best practice:
#   - keep all your tests tasks in a single file in the tests module.
#   1. you will need to import the tasks in order for the worker to find them
#   2. It i easier to not duplicate task names.  This is important,
#   because celery keeps globa state and it is (as far as I can tell)
#   impossible to scope test tasks to the test module (just like we always
#   want to scope test-related Things to the test itsrlf, so that they dont
#   leak into other test files.
#
# this is needed to load the declared test tasks into the worker.
import tests.integration.tasks

# While it;s incredibly convenient to run your integration
# tests without the broker (you can attch to a debugger),
# it's important to do so as part of CI, since there are
# things that can go wrong.  For instance, when running in eager
# mode your task arguments and return values are not serialized
# for transport over the network, and the tasks may fail when
# actually run in production.
# Because unit tests should never care about the broker, this
# is not an issue (see tests/unit/conftest.py for an example).
try:
    os.environ['PIPELINE_TEST_USE_BROKER']
except KeyError:
    EAGER = True
else:
    EAGER = False

class Config:
    BROKER_URL = 'redis://localhost:6380/0'
    CELERY_RESULT_BACKEND = 'redis://localhost:6380/0'
    # if set, integration tests will not use the broker
    CELERY_ALWAYS_EAGER = False #EAGER

app = Celery()
app.config_from_object(Config)