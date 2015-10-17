"""Dummy jinja2 filters for testing filter lookup.
"""
from jinja2 import contextfilter, evalcontextfilter, environmentfilter


def plain_function(arg):
    pass


@contextfilter
def context_filter(ctx, arg):
    pass


@evalcontextfilter
def eval_context_filter(eval_ctx, arg):
    pass


@environmentfilter
def environment_filter(arg):
    pass
