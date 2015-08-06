import pytest
import operator
from pipeline import (
    TaskAction, action, BuildContext
)
from pipeline.actions import register_action


import logging
logger = logging.getLogger(__name__)

@action
def stuff_increment_source(self, source, amount):
    """Print some stuff to the console."""
    return operator.add(source, int(amount))




@pytest.mark.xfail(reason='non-keyarg passing to tasks is no longer supported')
def test_prepare():
    @action(name='task_test_prepare', called=False)
    def task_test_prepare(self, context, some_arg=None):
        """Task returns what was passed in positional arg 1"""
        self.called = True
        return some_arg

    dct = {
        "name": "task_test_from_dict_action",
        "task": "task_test_prepare",
    }
    task_action = TaskAction.from_dict(dct)

    assert not task_action.partial
    task_action.prepare(build_context=None)
    assert task_action.partial

    assert not task_test_prepare.called
    ret = task_action.delay(some_arg=42).get()

    assert task_test_prepare.called
    assert ret == 42

class TestActions:
    """Tests for the ``pipeline.actions`` module.
    """

    def test_simple_action_from_dict(self):
        """Test that TaskAction.from_dict() with a simple action."""
        @action(called=False, name='test_task_simple')
        def test_simple(self, source):
            self.called = True
            return 42

        dct = {
            "name": "some_action",
            "task": "test_task_simple",
        }
        task_action = TaskAction(dct['task'], name=dct['name'])
        partial = task_action.prepare(None, BuildContext())
        ret = partial.delay().get()

        assert task_action.task.called
        assert ret.results['some_action'] == 42


    def test_callback_action_from_dict(self):
        """Test that TaskAction.from_dict() with a simple action.
        TESTING NOTES: here we are capturing passed args as instance vars,
        so we can make assertions about the behaviro after the fact.
        """
        @action(called=False, verify_kwargs={})
        def test_task_false(self, source, some_attribute=None):
            self.called = True
            self.verify_kwargs['some_attribute'] = some_attribute
            return False

        @action(called=False)
        def test_task_errback(self, source):
            self.called = True
            return 'BLAH'

        @action(called=False)
        def test_task_callback(self, source):
            self.called = True


        task_action = TaskAction('test_task_false', some_attribute=42)

        source = 12345
        partial = task_action.prepare(source, BuildContext())
        ret = partial.delay().get()

        # assert that action was called properly
        assert 'some_attribute' in task_action.task.verify_kwargs
        assert task_action.task.verify_kwargs['some_attribute'] == 42

        # assert that action was called properly;
        # it should not have a parent, and the source should
        # be set.
        assert task_action.task.called

        # assert that the initial action's return value was preserved
        # (i.e. the failure handler's return value is not used)
        assert isinstance(ret, BuildContext)

    @pytest.mark.xfail(reason='removed TaskAction.from_dict, so manually need to create the action with callbacks')
    def test_callback_action_from_dict_with_children(self):
        """Test that TaskAction.from_dict() with a simple action.
        TESTING NOTES: here we are capturing passed args as instance vars,
        so we can make assertions about the behaviro after the fact.
        """
        @action(called=False, verify_kwargs={})
        def test_task_false_with_children(self, some_attribute=None):
            self.called = True
            self.verify_kwargs['some_attribute'] = some_attribute

        @action(called=False)
        def test_task_errback_with_children(self):
            self.called = True

        @action(called=False)
        def test_task_callback_with_children(self):
            self.called = True


        dct = {
            "task": "test_task_false_with_children",
            "kwargs": {
                'some_attribute': 42,
            },
            "children": [
                {
                    "task": "test_task_errback_with_children",
                    "predicate": "not parent"
                },
                {
                    "task": "test_task_callback_with_children",
                }
            ]
        }
        task_action = TaskAction.from_dict(dct)

        source = 12345
        task_action.prepare(BuildContext(source))
        ret = task_action.delay().get()

        # assert that action was called properly
        assert 'some_attribute' in task_action.task.verify_kwargs
        assert task_action.task.verify_kwargs['some_attribute'] == 42

        # assert that action was called properly;
        # it should not have a parent, and the source should
        # be set.
        assert task_action.task.called

        # assert that failure handler was called
        assert task_action.children[0][0].task.called

        # assert that the success handler was not called
        assert not task_action.children[1][0].task.called

        # assert that the initial action's return value was preserved
        # (i.e. the failure handler's return value is not used)
        assert not ret.last

class TestActionsRegistration:
    """TEst action registration"""

    # def test_factory(self):
    #     """Test that I can register a task function,
    #     get that task from TaskAction.factory(), and call it."""
    #
    #     @action(called=False)
    #     def test_task_withargs(self):
    #         """A task that accepts one arg and one kwargs, and
    #         return 42."""
    #         self.called = True
    #         return 42
    #
    #     task = TaskAction.get(
    #         'test_task_withargs',
    #     )
    #     task.prepare(BuildContext(1))
    #     signature = task.s()
    #     ret = signature.delay().get()
    #
    #     assert task.called
    #     assert signature.kwargs == {'context': 1, 'kwarg': 9}
    #     assert ret.result == 42

    def test_class_registry(self):
        """Test that defining a TaskAction registers the task"""
        @action(called=False, register=False)
        def test_task_reg(self):
            self.called = True
            return 42

        class TestAction(TaskAction):
            _name = 'test_task_reg'
            _task = test_task_reg

        assert 'test_task_reg' in TaskAction._registry

    def test_class_register_manually(self):
        """Test that calling `register_task` registers the task"""
        @action(called=False, register=False)
        def test_task_reg2(self):
            self.called = True
            return

        register_action('test_task_reg2', test_task_reg2)

        assert 'test_task_reg2' in TaskAction._registry


class TestDecorator:
    """Test that `action` decorator."""

    def test_task_registered(self):
        """Test that @action decorator properly registers the
        task with the current app."""
        @action(name='i_should_be_registered')
        def test_task_should_be_reg(self):
            return

        from celery._state import get_current_app
        assert 'i_should_be_registered' in get_current_app().tasks

    # def test_task_created(self):
    #     """Test that @action decorator properly creates the
    #     task with the correct arguments."""
    #     @action(called=False)
    #     def test_task_created(self, context):
    #         self.called = True
    #         return 42
    #
    #     ret = test_task_created.delay(context=None).get()
    #     assert ret == 42, ret
    #     assert test_task_created.called

    def test_task_no_kwargs(self):
        """Test that @action decorator works w/o kwargs."""
        #with using_test_app() as app:

        @action
        def test_no_kwargs_task(self, context):
            return 42

        from celery._state import get_current_app
        assert "{}.test_no_kwargs_task".format(__name__) in get_current_app().tasks


