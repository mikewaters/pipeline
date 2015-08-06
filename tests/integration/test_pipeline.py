"""
NOTES:
    - No point in using a pytest fixture for the celery app, since celery
    sets up global state anyway
    - To differentiatetasks in tests, create them with a custom 'name'
    parameter.
    - It is not possible to tie tasks to an app. All apps will share all tasks,
    as long as they are part of a discoverabe module (i.e. part of a package).
    You can only override this behavior by providing a 'name' when defining the
    task.
    Fucking shit.

Features:
    -
"""
import pytest
from pipeline import Pipeline, TaskAction

import logging
logger = logging.getLogger(__name__)



def test_unnamed_action_in_pipeline():
    """Test that an unnamed action will get the module.task_name name."""
    actions = [
        TaskAction(
            'stuff_increment_source',
            amount='1'
         ),
    ]
    executor = Pipeline(actions)
    result = executor.schedule(1).get()
    assert 'stuff_increment_source' in result.results

def test_single_action_in_pipeline():
    """Test a single action scheduled by the executor.
    """
    actions = [
        TaskAction('stuff_increment_source',
            name= 'increment',
            amount='1'
        )
    ]
    executor = Pipeline(actions)
    result = executor.schedule(1).get()
    assert result.results['increment'] == 2

def test_two_actions_in_pipeline():
    """Test a single action scheduled by the executor.
    """
    actions = [
        TaskAction('stuff_increment_source',
            name= 'increment',
            amount='1'
        ),
        TaskAction('stuff_increment_source',
            name= 'increment_again',
            amount='{{ increment }}'
        )
    ]
    executor = Pipeline(actions)
    result = executor.schedule(1).get()
    assert result.results['increment_again'] == 3


def test_context_and_kwargs_application_in_pipeline():
    """Test that both build context and user-supplied kwargs are applied
    to a series of tasks in a chain.
    """
    actions = [
        TaskAction('increment', num='0'),
        TaskAction('increment', name='increment_again', num='{{ increment }}'),
        TaskAction('increment', name='increment_once_more', num='{{ increment_again }}')

    ]
    executor = Pipeline(actions)
    result = executor.schedule(None).get()
    assert result.results['increment_once_more'] == 3


def test_named_actions_in_pipeline():
    """Test that named actions store their name in build context.
    """
    dct = {
        "name": "mytask",
        "task": "named_action",
    }
    actions = [TaskAction(dct['task'], name=dct['name'])]

    executor = Pipeline(actions)
    ret = executor.schedule('42').get()

    assert 'mytask' in ret.results.keys()
    assert bool(ret.results['mytask'])

