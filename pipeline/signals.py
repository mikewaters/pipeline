"""
Pipeline celery signal handlers

"""
from copy import deepcopy

from celery import group
from celery.signals import task_postrun

from pipeline.actions import PipelineTask
from pipeline.eval import safe_eval

import logging
logger = logging.getLogger(__name__)



@task_postrun.connect()
def task_postrun_handler(*args, **kwargs):
    """Action post-run signal handler to execute handlers.
    """
    assert 'retval' in kwargs
    assert 'task' in kwargs
    assert 'sender' in kwargs
    assert 'state' in kwargs

    # skip failed tasks
    if kwargs['state'] != 'SUCCESS':
        return

    # skip tasks that aren't part of pipeline
    if not isinstance(kwargs['sender'], PipelineTask):
        return

    task_kwargs = deepcopy(kwargs['kwargs'])  # inception!
    state = task_kwargs.get('_pipeline_chain_state', {})

    if 'hooks' in state:

        child_tasks = state['hooks']
        logger.debug('task has hooks: {}'.format(child_tasks))
        context = task_kwargs['_pipeline_chain_state']['build_context']

        callbacks = []
        for action, predicate in child_tasks:
            try:
                should_execute = safe_eval(
                    predicate,
                    context.eval_context
                )
            except:
                should_execute = False

            if should_execute:
                source = kwargs['args'][0]  # wtf
                callbacks.append(action.prepare(source, context))
            else:
                logger.debug('hook {} should not execute.'.format(action))

        if len(callbacks):
            logger.debug('Executing some hooks: {}'.format(callbacks))
            canvas = group(*callbacks)
            canvas.apply_async()
