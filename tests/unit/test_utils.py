import pytest

from pipeline.utils import jinja_filters_from_module

from . import jinjafilters


def test_jinja_filter_lookup():
    """Test that our dummy jinja filters are discovered."""
    testfilters = '{}.{}'.format(
        '.'.join(__name__.split('.')[0:-1]), 'jinjafilters'
    )
    funcs = jinja_filters_from_module(
        testfilters
    )
    assert 'context_filter' in funcs
    assert funcs['context_filter'] == jinjafilters.context_filter

    assert 'eval_context_filter' in funcs
    assert funcs['eval_context_filter'] == jinjafilters.eval_context_filter

    assert 'environment_filter' in funcs
    assert funcs['environment_filter'] == jinjafilters.environment_filter

    assert 'plain_function' not in funcs
