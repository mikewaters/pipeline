"""
I am using a best practice of prepending test-local celery
tasks with an identifier, to eliminate task name collisions
in the global task registry.
"""
import operator
from pipeline.actions import TaskAction, action
from pipeline.executor import Executor

import logging
logger = logging.getLogger(__name__)

@action
def stuff_increment_source(self, source, amount):
    """Print some stuff to the console."""
    return operator.add(source, amount)

def test_single_action():
    """Test a single action scheduled by the executor.
    """
    actions = [
        TaskAction.from_dict({
            'action': 'stuff_increment_source',
            'name': 'increment',
            'kwargs': {
                'amount': 1
            }
        }),
    ]
    executor = Executor()
    result = executor.schedule(actions, 1).get()
    assert result == 2