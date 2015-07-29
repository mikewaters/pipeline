import os
import pwd
import six
import shutil
import logging                  # <-- beautiful
import tempfile                  # <-- imports
import platform                   # <-- inspired by
import functools                   # <-- miki725!
import subprocess
import contextlib
from copy import deepcopy

#TODO fix commandsession library import
from commandsession.commandsession import CommandSession

from django.conf import settings
from pipeline.bases import Registry

from pipeline.utils import rand_suffix


logger = logging.getLogger(__name__)


def get_current_user():
    """Return the current user.
    There are various ways to do this, and I've tried them all.
    """
    return pwd.getpwuid(os.getuid())[0]


def get_user_environment(username):
    """Get a user's environment as a dictionary.

    Requires that the current user has sudo privileges.

    :param username: a valid system username
    :returns: dict of env vars
    """
    if get_current_user() == username:
        cmd = 'env'
    else:
        cmd = 'sudo su -l {} -c env'.format(username)

    try:
        env_str = subprocess.check_output(
            cmd, stderr=subprocess.PIPE, shell=True
        )
    except subprocess.CalledProcessError as e:
        logger.error("Error getting user {}'s environment: {} {} {}".format(
            username, e.cmd, e.returncode, e.output.decode('utf-8'))
        )
        raise
    else:
        # `check_output` returns bytestring in py3
        # this is a nop in py2
        env_str = env_str.decode('utf-8')
        env = {
            l[0]:l[2] for l in
            [i.partition('=') for i in env_str.split()]
        }

        # hack: for some reason the `env` command is returning '/root' for hamster's homedir,
        # even though /etc/passwd is correct.
        #FIXME
        env['HOME'] = "/home/{}".format(username)

        if hasattr(settings, 'PIPELINE_INJECT_ENVIRON'):
            env.update(settings.PIPELINE_INJECT_ENVIRON)

        return env


class WorkspaceError(Exception):
    pass


class Workspace(metaclass=Registry):
    """Base class for Workspaces."""


class workspace(contextlib.ContextDecorator, Workspace):
    """Workspace.

    A container for interacting with the system on a worker.

    Creates/deletes on-disk working directory and handles
    execution of shell commands within the correct environment.

    Must be instantiated on the worker.
    """
    __id = 'workspace'

    def __init__(self, name=None, basepath=None, hints=None, delete=True, reusable=False, session=None, force_shell=True):
        """`reusable` param is only used for testig right now.

        :param name:
            combined with ``hints`` and the name of this class,
            determines the on-disk directory name for this workspace.
        :param basepath: location of temp area for creating workspace
        :param hints: list, see ``name``
        :param delete: boolean, determines whether to delete this workspace
            after use
        :param reusable: boolean, if False the on-disk directory has a
            random value prepended to it.
        :param session: CommandSession instance, if provided the workspace will
            use the given session, otherwise it will create one.  Useful
            if the cller wants to inspect the session log after-thefact.
        """
        self.user = get_current_user()

        if not basepath:
            basepath = settings.PIPELINE_WORKSPACE_ROOT


        logger.debug('initializing workspace for user {} in {}'.format(
            self.user, basepath
        ))

        self.delete = delete

        path_parts = hints or []
        assert isinstance(path_parts, list)

        if not reusable:
            path_parts.append(rand_suffix())

        prefix = [x for x in [self.__class__.__name__.lower(), name] if x]
        pathstr = '-'.join([str(x) for x in prefix + path_parts])

        self.location = self._cwd = os.path.join(
            basepath or tempfile.gettempdir(), pathstr
        )

        self.session = session or CommandSession(
            stream=True,
            env=self.environ,
            cwd=self._cwd,
            force_shell=force_shell
        )

    @property
    def cwd(self):
        return self._cwd

    @cwd.setter
    def cwd(self, directory):
        self._cwd = directory
        self.session._cwd = self._cwd

    @property
    def environ(self):
        """Get the user's environment."""
        env = get_user_environment(self.user)
        # haaaaaaaaaaaaaack for OSX w/ brew
        if platform.uname()[0] == 'Darwin':
            env['PATH'] = "{}:{}".format(
                '/usr/local/bin',
                env['PATH']
            )
        return env

    def acquire_source(self, source):
        """Run a source's acquisition instructions in the workspace.
        This may involve changing the workspace's working directory
        to the location of the acquired source.
        """
        if not hasattr(source, 'acquisition_instructions'):
            logger.debug('source {} does not need to be acquired'.format(source))
            return 

        instructions = source.acquisition_instructions

        self.session.check_call(instructions['command'])

        if 'directory' in instructions:
            self.cwd = os.path.join(
                self.location, instructions['directory']
            )
            logger.debug('cwd changed to {}'.format(self.cwd))

        for command in instructions['post_commands']:
            self.session.check_call(command)

    def __enter__(self):
        """Populate the workspace.
        """
        assert not os.path.exists(self.location)
        logger.debug('creating workspace {}'.format(self.location))
        try:
            os.mkdir(self.location)
        except OSError as exc:
            logger.error("Could not create workspace: %s" % str(exc))
            raise WorkspaceError(
                "Could not create workspace at %s" % self.location
            ) from exc

        return self

    def __exit__(self, *exc):
        # this is fricking dangerous.  figure something out.
        logger.debug('deleting workspace {}'.format(self.location))
        if self.delete:
            shutil.rmtree(self.location)

        return False


class python_workspace(workspace):
    """Python2-specific workspace.
    Wraps execution in a virtualenv, by prepending env directory to PATH.
    """
    __id = 'python_workspace'
    venv = 'virtualenv -p python2'

    @property
    def environ(self):
        env = super(python_workspace, self).environ
        pth = "{}:{}".format(
            os.path.join(self.location, 'env', 'bin'), env['PATH']
        )
        env['PATH'] = pth

        return env

    def __enter__(self):
        super(python_workspace, self).__enter__()
        
        ret = self.session.check_call("{} env".format(self.venv))
        if not ret == 0:
            raise WorkspaceError('Could not create virtualenv')

        self.environ['PATH'] = "{}:{}".format(
            os.path.join(self.location, 'env', 'bin'),
            self.environ['PATH']
        )
        self.environ['VIRTUAL_ENV'] = os.path.join(self.location, 'env')

        return self

class python3_workspace(python_workspace):
    """Python3-specific workspace.
    Wraps execution in a virtualenv, by prepending env directory to PATH.
    """
    __id = 'python3_workspace'
    venv = 'pyvenv'  #, '-p', 'python3']


def get_workspace(workspace_type, *args, **kwargs):
    """Temporary factory function."""
    if workspace_type in ('python', 'python2'):
        return python_workspace(*args, **kwargs)
    elif workspace_type == 'python3':
        return python3_workspace(*args, **kwargs)
    return workspace(*args, **kwargs)


