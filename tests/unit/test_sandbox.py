"""Sandbox for testing celery's behavior in certain scenarios.
"""
import pytest
from celery import  chain, task

pytest.mark.xfail(reason='this feature is no longer being used, TODO')
def test_mutable_chain_with_multiple_return_values():
    """See what happens when a task in a chain returns
    multiple values

    #NOTE: scoping tasks in a function is purely for readability,
    these tasks will still b global and will still persist after
    this test has completed.

    RESULT: all return values are provided in a tuple in args[0]
    """
    @task
    def task_that_returns_multiple_values():
        return 1, 2, 3, {'one': 1}

    @task
    def task_that_accepts_values(*args, **kwargs):
        print(args)
        print(kwargs)

    chain(
        task_that_returns_multiple_values.s(),
        task_that_accepts_values.s(),
    ).apply_async().get()

    assert 1==1





