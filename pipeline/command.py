
import subprocess

from pipeline.actions import action

import logging
logger = logging.getLogger(__name__)

__all__ = ['CommandSessionResult', 'shell_command']


class CommandSessionResult(object):
    """Class for encapsulating results of a shell 
    command session.
    """
    def __init__(self, session):
        #TODO add more stuff here
        self.output = session.last_output
        self.returncode = session.last_returncode
        self.log = session.log

@action
def shell_command(self, source, commands):
    """Run a sequence of shell commands.
    If any command returns nonzero, stop execution.
    """
    assert isinstance(commands, (list, tuple))

    with self._pipeline_workspace as workspace:
        for command in commands:
            logger.debug('Running command {}'.format(command))
            try:
                workspace.session.check_call(command)
            except subprocess.CalledProcessError as ex:
                logger.debug('Command {} failed with {}.'.format(
                    command, str(ex)
                ))
                break
            else:
                logger.debug("Command {} succeeded.".format(
                    command
                ))
            finally:
                logger.debug("Comand {} returned code {}".format(
                    command, workspace.session.last_returncode)
                )
        return CommandSessionResult(workspace.session)
