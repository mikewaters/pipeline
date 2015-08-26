import pytest
from pipeline import TaskAction, BuildContext, ActionHook
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

def test_action_with_single_default_post_hook():
    """Test executing an action having a post hook."""
    hook = ActionHook(
        'increment_call_count',
    )
    action = TaskAction(
        'increment_call_count',
        name='action1',
        hooks=[hook]
    )

    partial = action.prepare(42, build_context=BuildContext())
    partial.delay().get()

    assert increment_call_count.call_count == 2

def test_action_with_single_default_error_hook():
    """Test executing an action having an error hook."""
    hook = ActionHook(
        'increment_call_count',
        event='error'
    )
    action = TaskAction(
        'err',
        hooks=[hook]
    )

    partial = action.prepare(42, build_context=BuildContext())

    with pytest.raises(ValueError):
        partial.delay().get()

    assert increment_call_count.call_count == 1

def test_action_with_single_default_pre_hook():
    """Test executing an action having a pre hook."""
    hook = ActionHook(
        'increment_call_count',
    )
    action = TaskAction(
        'increment_call_count',
        name='action1',
        hooks=[hook]
    )

    partial = action.prepare(42, build_context=BuildContext())
    partial.delay().get()

    assert increment_call_count.call_count == 2

@pytest.mark.xfail(reason='TODO: Unsure how to revoke a task')
def test_action_with_single_default_revoke_hook():
    """Test executing an action having a revoke hook."""
    assert False

def test_action_with_multiple_default_post_hooks():
    """Test executing an action having a post hook."""
    hooks = [
        ActionHook(
            'increment_call_count',
        ),
        ActionHook(
            'increment_call_count',
        ),
        ActionHook(
            'increment_call_count',
        )
    ]
    action = TaskAction(
        'increment_call_count',
        name='action1',
        hooks=hooks
    )

    partial = action.prepare(42, build_context=BuildContext())
    partial.delay().get()

    assert increment_call_count.call_count == 4