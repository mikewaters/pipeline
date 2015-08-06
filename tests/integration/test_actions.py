import pytest
from pipeline import TaskAction, BuildContext
from .tasks import increment_call_count



def test_action(scoped_broker):
    """Test a single action is executed."""
    #increment_call_count.call_count = 0
    action1 = TaskAction(
        'increment_call_count',
        name='action1'
    )
    action2 = TaskAction(
        'increment_call_count',
        name='action2'
    )
    partial = action1.prepare(42, build_context=BuildContext())
    partial.delay().get()

    partial = action2.prepare(42, build_context=BuildContext())
    partial.delay().get()

    assert increment_call_count.call_count == 2

def test_action2(scoped_broker):
    """Test a single action is executed."""
    #from .tasks import increment_call_count
    #increment_call_count.call_count = 0
    action = TaskAction(
        'increment_call_count'
    )

    partial = action.prepare(42, build_context=BuildContext())
    ret = partial.delay().get()
    assert increment_call_count.call_count == 1
