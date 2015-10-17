from celery import chain, group

from pipeline.context import BuildContext

import logging
logger = logging.getLogger(__name__)

__all__ = ['Pipeline']


class Pipeline(object):
    """Abstraction of an execution pipeline.
    """
    def __init__(self, source, actions, composition='chain', context_klass=BuildContext):
        self.source = source
        self.actions = actions
        self.composition = composition
        self.context = context_klass()

    def schedule(self):
        """Schedule actions using celery.
        """
        tasks = []
        if self.composition == 'chain':
            composer = chain
        elif self.composition == 'group':
            composer = group
        else:
            raise ValueError('Unknown composition type {}'.format(self.composition))

        for idx, action in enumerate(self.actions):
            if (self.composition=='chain' and idx == 0) or (self.composition=='group'):
                # In the case of synchronous execution (chain), the first action
                # must be 'primed' with a build context (subsequent actions
                # will recieve a copy of this context from the previously executed action).
                # In the case of parallel execution (group), each action needs
                # a build context of it's own, that it can pass to it's child tasks/callbacks.
                build_context = self.context
                partial = action.prepare(self.source, build_context)
            else:
                partial = action.prepare(self.source)

            tasks.append(partial)

        canvas = composer(*tasks)

        logger.debug('got {} actions {}'.format(len(self.actions), self.actions))
        logger.debug('scheduling tasks {} using {}'.format(tasks, composer))

        ret = canvas.apply_async()

        return ret