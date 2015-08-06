import pytest
from pipeline import TaskAction, BuildContext
from .tasks import increment_call_count

pytestmark = pytest.mark.usefixtures('scoped_broker')


def test_single_action():
    """Test a single action is executed."""

    action = TaskAction(
        'increment_call_count',
        name='action1'
    )

    partial = action.prepare(42, build_context=BuildContext())
    partial.delay().get()

    assert increment_call_count.call_count == 1

def test_multiple_actions():
    """Test multiple action is executed."""

    actions = [
        TaskAction(
            'increment_call_count'
        ),
        TaskAction(
            'increment_call_count',
            name='needsanameoritwillfail'
        )
    ]
    for action in actions:
        partial = action.prepare(42, build_context=BuildContext())
        partial.delay().get()

    assert increment_call_count.call_count == 2

def test_action_with_single_callback():
    """Test executing an action having a callback hook."""
