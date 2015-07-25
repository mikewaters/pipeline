import os
import json
import subprocess

from pipeline.bases import Registry
from pipeline.workspace import Workspace
from pipeline.actions import action, TaskResult


import logging
logger = logging.getLogger(__name__)


@action
def shell_command(self, source, commands):
    """Run some shell commands.
    Holy shit this is easy.
    Note that these are like commands in a makefile, in that there is
    no state transferred from one line to another (liek working directory etc)
    """
    assert isinstance(commands, (list, tuple))

    with self._pipeline_workspace as workspace:
        workspace.acquire_source(source)
        for command in commands:
            try:
                workspace.session.check_call(command)
            except subprocess.CalledProcessError as ex:
                logger.critical(str(ex))
                break

        return workspace.session.log


class BaseOutputFormatter(object):
    def __call__(self, output):
        return output.strip()


class LineOutputFormatter(BaseOutputFormatter):
    """The interface of this formatter is that it should accept
    unicode input, and return a list of strings.
    """
    def __call__(self, output):
        """Return list-ified output."""
        return super(LineOutputFormatter, self).__call__(output).split(os.linesep)


class CommandResult(TaskResult):
    """Subclass of TaskResult that understands that shell commands
    return 0 for success."""
    def __init__(self, return_value, output=None):
        #self.return_value = bool(return_value==0)
        self.returncode = return_value
        super(CommandResult, self).__init__(bool(return_value==0), output=str(output))
    #def __bool__(self):
    #    return self.return_value == 0


class BakedCommand(metaclass=Registry):
    """Base class for shell commands."""
    __id = None  # required
    install = None  # optional
    # removed until I figure out how to transform input
    formatter = BaseOutputFormatter

    def __init__(self, context):
        #self.source = source
        #self.retval = retval
        self.context = context

    def instructions(self):
        """Command entry point.
        :param source: required
        :returns list of subprocess-friendly lists
            ex:
                return [
                    ['ls', '-l'],
                    ['uname', '-a']
                ]
        """
        assert hasattr(self, 'context')
        raise NotImplementedError


class CommandError(Exception):
    pass


def baked_command_task(self, context, **kwargs):
    """This is the template function for a task that will run
    a shell command inside of a ``pipeline.workspace.workspace``.

    This wont work by itself, needs to be wrapped with shared_task
    (which is accomplished by calling ``register_command_task``).

    TODO: fix error handling inthis module, defer command errors to workspace.py?

    """
    assert hasattr(self, 'workspace_class')
    assert hasattr(self, 'command_name')

    workspace_cls = Workspace.get(self.workspace_class)
    with workspace_cls(name='command-{}'.format(self.command_name)) as wkspc:

        # we pass source everydamnwhere.  0xbadidea?
        cmd = BakedCommand.factory(self.command_name, context=context)

        #TODO: use check_call here so I can catch CalledProcessError
        # instead of these nested if stmts.
        result = wkspc.acquire_source(context.source)

        if result == 0:
            if cmd.install:
                install_ret = wkspc.run_command(cmd.install)
                if not install_ret == 0:
                    raise CommandError("error {} running {} as {} with {}".format(
                        install_ret, cmd.install, wkspc.user, json.dumps(wkspc.environ, indent=2)
                    ))

            #TODO: support multiple insturctions
            output = []
            assert len(cmd.instructions())
            for instruction in cmd.instructions():
                #instruction = cmd.instructions()[0]  # just grab the first
                _output, errcode = wkspc.get_output(instruction)
                output.append(cmd.formatter()(_output))
                logger.debug('command {} returned {}'.format(instruction, errcode))
            output = '\n'.join(output)
        else:
            #TODO: return output?
            raise CommandError("error {} running {} as {} with {}".format(
                result, context.source.acquisition_instructions, wkspc.user, json.dumps(wkspc.environ, indent=2)
            ))

        return TaskResult(errcode==0, errcode=errcode, output=output)


def register_baked_command_task(command_name, workspace_class='workspace', task_func=None, **kwargs):
    """Generate and register a TaskAction that wraps a shell command.

    :param command_name: the friendly task name that will
        be stored in the registry. Also will be used as celery
        internal task name.
    :param workspace_class: friendly/registered name of a subclass of
        ``pipeline.workspace.workspace``.  This will be used to wrap the
        execution of the command on a worker.
    :param task_func: a template function to be used to create the
    command task.  Primarily used for testing.
    :param kwargs: passed into ``shared_task`` to help define the task object.
    :returns: ``celery.local.Proxy`` instance - thsi represents the actual
        task stored inside the celery app.
    """
    if not task_func:
        task_func = baked_command_task

    assert hasattr(task_func, '__call__')

    return action(
        task_func,
        name=command_name,
        command_name=command_name,
        workspace_class=workspace_class,
        **kwargs
    )
