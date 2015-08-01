
from pipeline.actions import TaskAction
from pipeline.pipeline import Pipeline #TODO remove, need to test without it


class DummySource(object):
    __id = 'dummy'
    acquisition_instructions = {
        'command': 'pip install flake8-diff'
    }

def test_source_acquired():
    """Test that a single shell command is executed.
    This will acquire a source that installs flake8-diff as part
    of it's acquisition instructions, and then verify that
    flake8-diff is installed in the task itself.
    """
    actions = [
        TaskAction(
            'shell_command',
            name= 'installer',
            # workspace= 'python3',
            # workspace_kwargs= {
            #     "delete": False
            # },
            commands= [
                'pip freeze |grep flake8-diff',
            ]

        ),
    ]
    executor = Pipeline(actions)
    source = DummySource()

    result = executor.schedule(source).get()
    assert result.results['installer'].returncode == 0

def test_shell_command_exit():
    """Test that a single shell command functions properly
    """
    actions = [
        TaskAction(
            'shell_command',
            name= 'exiter',
            commands= [
                'exit 1',
            ]

        ),
    ]
    executor = Pipeline(actions)
    source = DummySource()

    result = executor.schedule(source).get()
    assert result.results['exiter'].returncode == 1