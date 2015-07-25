
from pipeline.command import BakedCommand, register_baked_command_task
from pipeline.actions import TaskAction
from pipeline.context import BuildContext
from pipeline.executor import Executor


class DummySource(Source):
    __id = 'dummy'
    acquisition_instructions = [
        'pip install flake8-diff'
    ]

def test_single_shell_command_with_defaults():
    """Test that a single shell command is executed.
    """
    actions = [
        TaskAction.from_dict({
            'action': 'shell_command',
            'name': 'echoer',
            'workspace': 'python3',
            "workspace_kwargs": {
                "delete": False
            },
            'kwargs': {
                'commands': [
                    'pip freeze',
                ]
            }
        }),
    ]
    executor = Executor()
    source = DummySource()

    result = executor.schedule(actions, source).get()


class TestCommand(BakedCommand):
    __id = 'rarbl'
    def instructions(self):
        return self.context.source

def test_echo():
    """Test that shell command execution of echo works.
    Uses tasks defined in ``pipeline.tests.debug_module``
    """
    task = TaskAction.get('echo_test_command')

    # send the string '42' to the echo task
    ret = task.delay(context=BuildContext('42')).get()

    assert ret  # indicates success
    assert ret.output == '42'  # return value of echo command



def mock_command_task(self, context):
    """This is a replacement for ``pipeline.command.command_task``
    that doesnt actualy set up a workspace etc..."""
    self.called = True
    cmd = BakedCommand.factory(self.command_name, context=context)
    return cmd.instructions()


def test_command_registration():
    """Test command subclass registration."""
    class GlarblCommand(BakedCommand):
        __id = 'glarbl'

    assert 'glarbl' in BakedCommand._registry

def test_register_command():
    """Test the behavior of register_command.

    Has to be run in eager mode, as we can't make
    assertions about taks run in another process (the worker).
    """
    assert 'rarbl' not in TaskAction._registry

    register_baked_command_task(
        'rarbl',
        task_func=mock_command_task,
        called=False
    )
    assert 'rarbl' in TaskAction._registry

    task = TaskAction.get('rarbl')

    assert task.name == 'rarbl'
    assert hasattr(task, 'workspace_class')

    ret = task.delay(context=BuildContext('42')).get()
    assert task.called
    assert ret == '42'


