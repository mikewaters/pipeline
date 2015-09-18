"""
Pipeline celery signal handlers

TODO: restrict hooks from having hooks; could end up in an infinite loop
if a task is given itself as a hook.
"""
from copy import deepcopy

from celery import group
from celery.signals import task_postrun, task_failure, task_revoked, task_prerun

from pipeline.actions import PipelineTask
from pipeline.criteria import safe_eval

import logging
logger = logging.getLogger(__name__)


def execute_hooks(event_name, *args, **kwargs):
    """Execute action hooks based upon parent task state.
    """
    # skip tasks that aren't part of pipeline
    if not isinstance(kwargs['sender'], PipelineTask):
        return

    task_kwargs = deepcopy(kwargs['kwargs'])  # inception!
    state = task_kwargs.get('_pipeline_chain_state', {})

    if 'hooks' in state:

        hooks = [
            h for h in state['hooks'] if \
            h.event == event_name
        ]

        if not hooks:
            return []

        logger.debug('task has hooks: {}'.format(hooks))
        context = task_kwargs['_pipeline_chain_state']['build_context']

        callbacks = []

        for hook in hooks:
            try:
                should_execute = safe_eval(
                    hook.predicate,
                    context.eval_context
                )
            except:
                should_execute = False

            if should_execute:
                source = kwargs['args'][0]  # wtf
                callbacks.append(hook.task_action.prepare(source, context))
            else:
                logger.debug('hook {} should not execute.'.format(hook.task_action))

        if len(callbacks):
            logger.debug('Executing some hooks: {}'.format(callbacks))
            canvas = group(*callbacks)
            return canvas.apply_async()

    return []


@task_postrun.connect
def task_success_handler(*args, **kwargs):
    """We use the postrun handler instead of success
    handler because we require the original task arguments in order
    to access the chain state (where the hooks are stored).
    The success handler does not send this.
    """
    if kwargs['state'] == 'SUCCESS':
        execute_hooks('post', *args, **kwargs)


@task_revoked.connect
@task_failure.connect
def task_failure_handler(*args, **kwargs):
    execute_hooks('error', *args, **kwargs)


@task_prerun.connect
def task_prerun_handler(*args, **kwargs):
    execute_hooks('pre', *args, **kwargs)