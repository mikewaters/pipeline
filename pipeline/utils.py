import os
import pwd
import random
import logging
import importlib
import inspect
import subprocess

logger = logging.getLogger(__name__)


def jinja_filters_from_module(module_path):
    """Acquire the names of jinja filters in a given module.
    :param module_path: pth to module, e.g. "package.module.file"
    :returns: {'name': 'importable path'} dict
    """
    filters = {}

    try:
        mod = importlib.import_module(module_path)
    except ImportError:
        return filters

    for name, func in inspect.getmembers(mod):
        for attr in ('contextfilter', 'evalcontextfilter', 'environmentfilter'):
            if inspect.isfunction(func) and hasattr(func, attr):
                filters[name] = func

    return filters


def rand_suffix():
    """Generate a workspace suffix.
    :returns: random 8-character hex string
    """
    return '%08x' % random.randrange(16**8)


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

        return env