""" Pipeline Executors.

They accept a list of actions, and execute them in some order using the
provided Source and a BuildContext.

"""
from celery import chain

from pipeline.context import BuildContext

import logging
logger = logging.getLogger(__name__)


class Executor(object):
    """Task scheduler.  Currently using a single chain,
    but a future impl will use more of the celery canvas (perhaps allowing the
    caller to specify, or using a dependency graph to use the most efficient
    canvas)."""
    @staticmethod
    def schedule(actions, source, **kwargs):
        """Execute actions using source, return the AsyncResult of the last task."""
        tasks = []
        for idx, action in enumerate(actions):
            if idx == 0:
                # Only pass in context for the first task in the chain,
                # as celery mutable signatures will take care
                # of propagating it to the other tasks.
                build_context = BuildContext(**kwargs)
                partial = action.prepare(source, build_context)
            else:
                partial = action.prepare(source)

            tasks.append(partial)  # action.partial)

        canvas = chain(*tasks)

        logger.debug('got {} actions {}'.format(len(actions), actions))
        logger.debug('chaining tasks {}'.format(tasks))

        ret = canvas.apply_async(retry=False)  #FIXME retry policy

        return ret

