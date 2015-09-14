import pytest

from pipeline.eval import evaluate_single_criterion, Matcher, matcher

class Source(object):
    pass

def test_custom_matcher():
    """Test that a custom criteria matcher functions."""
    class TestMatcher(Matcher):
        __id = "test_matcher"
        def __call__(self, criteria):
            return criteria == 'qwerty'

    source = Source()
    setattr(source, 'test', 'qwerty')

    assert evaluate_single_criterion(
        {'source': source},
        ['source.test', 'matches', 'test_matcher']
    )

def test_register_matcher():
    """Test that matcher decorator works."""
    @matcher
    def x():
        pass

    assert 'x' in Matcher._registry


def test_config_eval():
    """Tests for the evaluate_single_criterion function."""
    source = Source()
    setattr(source, 'test', 'qwerty')
    assert evaluate_single_criterion(
        {'source': source},
        ['source.test', 'is', 'qwerty']
    )
    assert not evaluate_single_criterion(
        {'source': source},
        ['source.test', 'is', 'sqwerty']
    )
    assert evaluate_single_criterion(
        {'source': source},
        ['source.test', 'in', ['qwerty']]
    )
    assert evaluate_single_criterion(
        {'source': source},
        ['source.test', 'in', ['qwerty', 'uiop']]
    )
    assert not evaluate_single_criterion(
        {'source': source},
        ['source.test', 'in', ['qwert', 'uiop']]
    )
    assert evaluate_single_criterion(
        {'source': source},
        ['source.test', 'like', 'qwerty']
    )
    assert evaluate_single_criterion(
        {'source': source},
        ['source.test', 'like', '.werty']
    )
    assert evaluate_single_criterion(
        {'source': source},
        ['source.test', 'like', '.*wert.*']
    )
    assert evaluate_single_criterion(
        {'source': source},
        ['source.test', 'like', '.wert.']
    )
    assert evaluate_single_criterion(
        {'source': source},
        ['source.test', 'like', '[q]werty']
    )
    assert evaluate_single_criterion(
        {'source': source},
        ['source.test', 'like', '^qwerty$']
    )
    assert not  evaluate_single_criterion(
        {'source': source},
        ['source.test', 'like', '^qwert$']
    )
    assert not  evaluate_single_criterion(
        {'source': source},
        ['source.test', 'like', '..werty']
    )
    with pytest.raises(NotImplementedError):
        evaluate_single_criterion(
            {'source': source},
            ['source.test', 'boom', 'x']
        )