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
from pipeline import Pipeline, TaskAction, action

import logging
logger = logging.getLogger(__name__)


@action
def increment(self, source, num='1', by='1'):
    """An action that increments a value.
    """
    return int(num) + int(by)



def test_context_and_kwargs_application():
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


def test_named_actions():
    """Test that named actions store their name in build context.
    """
    @action(called=False)
    def named_action(self, source):
        self.called = True
        return True

    dct = {
        "name": "mytask",
        "task": "named_action",
    }
    actions = [TaskAction(dct['task'], name=dct['name'])]

    executor = Pipeline(actions)
    ret = executor.schedule('42').get()

    assert 'mytask' in ret.results.keys()
    assert bool(ret.results['mytask'])