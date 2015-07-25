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

    def __init__(self, name=None, basepath=None, hints=None, delete=True, reusable=False, session=None):
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

        self.location = self.cwd = os.path.join(
            basepath or tempfile.gettempdir(), pathstr
        )

        for k ,v in self.environ.items():
            logger.error("{}:{}".format(k, v))
            
        self.session = session or CommandSession(
            stream=True,
            env=self.environ,
            cwd=self.cwd
        )

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
        """
        if not hasattr(source, 'acquisition_instructions'):
            logger.debug('source {} does not need to be acquired'.format(source))
            return 0

        for item in source.acquisition_instructions:
            self.session.check_call(item)


    def __enter__(self):
        """Populate the workspace.
        """
        assert not os.path.exists(self.location)
        logging.debug('creating workspace {}'.format(self.location))
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
        logging.debug('deleting workspace {}'.format(self.location))
        if self.delete:
            shutil.rmtree(self.location)

        return False


class python_workspace(workspace):
    """Python2-specific workspace.
    Wraps execution in a virtualenv, by prepending env directory to PATH.
    """
    __id = 'python_workspace'
    venv = ['virtualenv', '-p', 'python2']

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
        
        ret = self.session.check_call(self.venv + ['env'])
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
    venv = ['pyvenv']  #, '-p', 'python3']


def get_workspace(workspace_type, *args, **kwargs):
    """Temporary factory function."""
    if workspace_type in ('python', 'python2'):
        return python_workspace(*args, **kwargs)
    elif workspace_type == 'python3':
        return python3_workspace(*args, **kwargs)
    return workspace(*args, **kwargs)


class v1_workspace(contextlib.ContextDecorator, Workspace):
    """DEPRECATED

    Workspace.

    A container for interacting with the system on a worker.

    Creates/deletes on-disk working directory and handles
    execution of shell commands within the correct environment.

    TODO:
        - implement workspace reusability. will allow us to leverage
        data locality, to execute commands in a pre-existing workspace
        eliminating startup costs
        - allow caller to specify username?
        - doesnt run bash with startup file etc..
        - file descriptors for stdout and stderr so that entire command
        output can be propagated to other tasks
        - experiment with various methods of subprocess.  currently using call(),
        which is simple, but it might not be enough.  for instance, capturing
        error and returning to caller, instead of diaplying to stderr of process

    """
    __id = 'v1_workspace'

    def __init__(self, name=None, basepath=None, hints=None, delete=True, reusable=False):
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

        """
        self.user = get_current_user()
        self.environ = get_user_environment(self.user)

        if not basepath:
            basepath = settings.PIPELINE_WORKSPACE_ROOT

        # haaaaaaaaaaaaaack for OSX w/ brew
        if platform.uname()[0] == 'Darwin':
            self.environ['PATH'] = "{}:{}".format(
                '/usr/local/bin',
                self.environ['PATH']
            )

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

        self.location = self.cwd = os.path.join(
            basepath or tempfile.gettempdir(), pathstr
        )

    def acquire_source(self, source):
        """Run a source's acquisition instructions in the workspace.
        If the source specifies that the installation resulted in
        a change of directory, alter self.cwd to match.

        #TODO use some abstraction for the instructions, instead of a dict
        """
        if not hasattr(source, 'acquisition_instructions'):
            logger.debug('source {} does not need to be acquired'.format(source))
            return 0

        instructions = source.acquisition_instructions

        # run install command
        # FIXME SOON
        if isinstance(instructions['command'], six.string_types):
            # hack for debugging
            import subprocess
            ret = subprocess.call(
                instructions['command'],
                shell=True
            )
        else:
            ret = self.run_command(
                instructions['command'],
            )

        if not ret == 0:
            logger.error("couldnt run command {} for source {}".format(
                instructions['command'], source
            ))
            return ret

        # update workspace location with source subdir.
        # this may not work, good enough for now.
        if instructions['directory']:
            self.cwd = os.path.join(
                self.location, instructions['directory']
            )
            logger.debug('cwd changed to {}'.format(self.cwd))

        # run post-install commands, if any
        for command in instructions['post_commands']:
            ret = self.run_command(command)
            if not ret == 0:
                logger.error("couldnt run command for source {}".format(
                    command, source
                ))
                return ret

        return 0

    def __enter__(self):
        """Populate the workspace.
        """
        assert not os.path.exists(self.location)
        logging.debug('creating workspace {}'.format(self.location))
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
        logging.debug('deleting workspace {}'.format(self.location))
        if self.delete:
            shutil.rmtree(self.location)

        return False

    def wrap_command(self, command, env=None):
        """Get a partial for running an arbitrary command.
        TODO: remove this. his was useful when we passed
        the callables into the celery task to micromanage them.
        """
        #assert isinstance(command, list), "not supporting subprocess shell=True"

        environ = deepcopy(self.environ)
        if env:
            environ.update(env)
        if isinstance(command, six.string_types):
            return functools.partial(
                subprocess.call,
                command, cwd=self.cwd, env=environ, shell=True
            )
        else:
            return functools.partial(
                subprocess.call,
                command, cwd=self.cwd, env=environ
            )

    def run_command(self, command, env=None):
        """Run a wrapped command."""
        logger.debug('running command {}'.format(command))
        command = self.wrap_command(command, env)
        #FIXME
        try:
            result = command()
        except Exception as e:
            logger.error(str(e))
            logger.error(self.environ)
            raise
        else:
            return result

    def get_output(self, command, env=None):
        """Temporary."""
        def fmt_output(what):
            return what.decode('utf-8')

        #assert isinstance(command, list), "not supporting subprocess shell=True"
        environ = deepcopy(self.environ)
        if env:
            environ.update(env)
        try:
            if isinstance(command, six.string_types):
                ret = subprocess.check_output(
                    command, cwd=self.cwd, env=environ, shell=True
                )
            else:
                ret = subprocess.check_output(
                    command, cwd=self.cwd, env=environ
                )
        except subprocess.CalledProcessError as exc:
            return fmt_output(exc.output), exc.returncode

        return fmt_output(ret), 0


