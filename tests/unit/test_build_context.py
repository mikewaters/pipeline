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
from pipeline.actions import TaskAction, action, TaskResult
from pipeline.executor import Executor

import logging
logger = logging.getLogger(__name__)

@action
def task_removal(self):
    pass

def test_removal():
    configurations = [
        {"name": "test", "task": "task_removal"},
    ]
    actions = [TaskAction.from_dict(dct) for dct in configurations]
    executor = Executor()
    result = executor.schedule(actions, 1).get()

@action
def increment_source(self, by=1):
    """An action that increments the provided source.
    """
    value = context.source + by
    return context.update(self, TaskResult(True, value=value))


@action
def increment_parent(self, context, by=1):
    """An action that extracts a value from parent and increments it.
    """
    value = context.parent.value + by
    return context.update(self, TaskResult(True, value=value))


@action
def increment_parent_again(self, context, by=1):
    """An action that extracts a value from parent and increments it.

    Required until we support user-defined task names.
    See TODO in ``pipeline.context`` module docstring.
    """
    value = context.parent.value + by
    return context.update(self, TaskResult(True, value=value))


def test_context_application():
    """Test that build context is applied to a series of tasks in a chain.
    """
    source = 0
    configurations = [
        {"task": "increment_source"},
        {"task": "increment_parent"},
        {"task": "increment_parent_again"}
    ]
    actions = [TaskAction.from_dict(dct) for dct in configurations]
    executor = Executor()
    result = executor.schedule(actions, source).get()
    assert result.last.value == 3


def test_context_and_kwargs_application():
    """Test that both build context and user-supplied kwargs are applied
    to a series of tasks in a chain.
    """
    source = 0
    configurations = [
        {"task": "increment_source", "kwargs": {"by": 2}},
        {"task": "increment_parent"},
        {"task": "increment_parent_again"}
    ]
    actions = [TaskAction.from_dict(dct) for dct in configurations]
    executor = Executor()
    result = executor.schedule(actions, source).get()
    assert result.last.value == 4

def test_named_actions():
    """Test that named actions store their name in build context.
    """
    @action(called=False)
    def named_action(self, context):
        self.called = True
        context.update(self, True)
        return context

    dct = {
        "name": "mytask",
        "task": "named_action",
    }
    actions = [TaskAction.from_dict(dct)]

    executor = Executor()
    ret = executor.schedule(actions, '42').get()

    assert 'mytask' in ret.results.keys()
    assert bool(ret.results['mytask'])