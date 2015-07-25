"""
Pipeline tasks and task helpers.

Place all code that interacts with celery shared_task or app.task in this module;
these will have side-effects, and it's good to keep them in a single module so that errant
utility imports don't trigger these side effects.
"""
import celery
from celery.canvas import Signature
import six

"""
Code below this point is fully functional, and is the result of a large amount of
analysis, reading celery source code, and testing.  But, pipeline does not need it.
Dammit.
"""
@celery.task(bind=True)
def task_predicate_match(self, *args, **kwargs):
    """REQUIRES Celery 3.2

    Schedule one of two tasks, based on evaluation of a predicate
    using the result of a parent task as context.
    If this is created as an immutable signature, this will not function.
    Discards the current task.

    :param *args: 2- or 3-tuple
        1. result of previous task (if this task is not immutable)
        2. task signature to execute on success
        3. task signature to execute on failure
    :param kwargs['predicate']: expression to eval to determine success
    :return: None
        Callers should not expect the return value of this
        task to be useful/usable, since it is being replaced.

    It is important to note that this task does not return a value,
    so any signature that follows it in a chain will not receive a result,
    and if this signature is the last in a chain it will not return
    any result to the original caller.
    """
    assert len(args) in (2,3)

    idx = 0
    if not isinstance(args[0], Signature):
        # signature for the current task is not immutable, so we
        # should assume that the first arg is the result of the
        # previous task in a chain.
        result = args[0]
        idx += 1

    # ^^ note that `result` is not part of local scope if we are
    # immutable; since it's only used for the predicate, we will allow
    # the eval chec to raise an exception, which will fail this task.

    success = args[idx]
    failure = args[idx+1]

    # default behavior is to evaluate `result` for truthy-ness
    predicate = kwargs.get('predicate', "result")


    assert not success or isinstance(success, Signature)
    assert not failure or isinstance(failure, Signature)
    assert success or failure
    assert isinstance(predicate, six.string_types)

    try:
        predicate_met = eval(predicate)
    except Exception as exc:
        # you may not recv a report of this exception
        # task_logger.error('cannot evaluate predicate {} ({})'.format(
        #     predicate,
        #     str(exc))
        # )
        raise
    else:
        if predicate_met and success:
            # task_logger.error('replacing task {} with {}'.format(
            #     self.name, success.task
            # ))
            self.replace(success)
        elif failure:
            # task_logger.error('replacing task {} with {}'.format(
            #     self.name, failure.task
            # ))
            self.replace(failure)


def exclusive_group(success, failure, **kwargs):
    """Conditionally schedule one or another task
    based on some predicate.

    :param success: celery signature/canvas element
    :param failure: celery signature/canvas element
    :param kwargs['predicate']: expression
    """
    # we send two params to task, it gets 3.  Magic!
    return task_predicate_match.s(success, failure, **kwargs)


def conditional_task(task, **kwargs):
    """Conditionally schedule a task based on a
    given predicate.

    :param task: celery signature/canvas element
    :param kwargs['predicate']: expression
    """
    return task_predicate_match.s(task, None, **kwargs)


