"""
Pipeline celery signal handlers

"""
from copy import deepcopy

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

    logger.debug('postrun handler triggered {} {}'.format(args, kwargs))

    task_kwargs = deepcopy(kwargs['kwargs'])  # inception!
    state = task_kwargs.get('_pipeline_chain_state', {})

    if 'children' in state:
        child_tasks = state['children']
        #TODO: check to make sure children never reaches beyond a single action
        del state['children']  # may not be necessary

        context = task_kwargs['_pipeline_chain_state']['build_context']

        for action, predicate in child_tasks:
            #TODO: execute children in parallel

            try:
                should_execute = safe_eval(
                    predicate,
                    context
                )
            except:
                should_execute = False

            if should_execute:
                action.prepare(context)
                action.partial.delay()
