"""
Evaluation helpers.


"""
import re

from pipeline.registry import Registry

ALLOWED_BUILTINS = ('bool',)

__all__ = ['matcher', 'safe_eval', 'evaluate_criteria', 'evaluate_single_criterion']

class Matcher(metaclass=Registry):
    """Base class for a criteria matcher.
    """
    def __call__(self, data):
        """Determine if `data` meets some expectations.
        :param data: data to use in match
        :returns: bool
        """
        raise NotImplementedError


def get_default_builtins():
    """Get the allowed eval() builtins.
    """
    ret = {}
    for k, v in __builtins__.items():
        if k in ALLOWED_BUILTINS:
            ret[k] = v
    return ret


def safe_eval(expression, _locals, _globals=None):
    """Run eval in a semi-safe manner.

    Current impl just runs eval().  Future iterations will attempt to
    do this in a safer manner, using ast.
    see: http://code.activestate.com/recipes/364469-safe-eval/
    """
    if not _globals:
         _globals = {'__builtins__': get_default_builtins()}

    _locals = _locals or {}

    # `expression` might just be a string, so catch some exceptions here
    # and just return expression.
    try:
        ret = eval(
            expression,
            _globals,
            _locals
        )
    except NameError:
        return expression
    except Exception as ex:
        print(ex)
        return expression
    return ret


def evaluate_single_criterion(data, criterion):
    """Determine if a one of criteria match the event data.
    This is a very, very trivial implementation of something
    that could be interesting, if given time to hack on it.

    An example of criteria is:
        ['object.attribute', 'is', 'some value']

    :param data: dict of event, source
    :param criterion: 3-tuple of (lvalue, operator, rvalue)
    :returns boolean
    """
    oper = criterion[1]
    try:
        lvalue = safe_eval(criterion[0], data)
    except:
        #TODO
        raise
    rvalue = criterion[2]

    if oper == 'is':
        return lvalue == rvalue
    elif oper == 'in':
        return lvalue in rvalue
    elif oper == 'like':
        #TODO re.flags?
        return bool(re.search(
            rvalue, lvalue
        ))
    elif oper == 'not':
        return lvalue != rvalue
    elif oper == 'not in':
        return lvalue not in rvalue
    elif oper == 'not like':
        #TODO re.flags?
        return not bool(re.search(
            rvalue, lvalue
        ))
    elif oper == "matches":
        # custom criteria matching
        matcher_klass = Matcher.get(rvalue)
        if not matcher_klass:
            raise NotImplementedError(
                'matcher {} not found'.format(rvalue)
            )
        return matcher_klass()(lvalue)
    else:
        raise NotImplementedError(
            'operator {} not supported'.format(oper)
        )


def evaluate_criteria(data, criteria):
    """Check to see if the criteria matches data.

    - if criteria is None, it should always match
    - if criteria is [], it should never match
    - otherwise, evaluate against deserialized incoming data

    :returns: bool
    """
    if criteria is None:
        return True

    assert isinstance(criteria, (list, tuple))
    if not len(criteria):
        return False

    for criterion in criteria:
        if not evaluate_single_criterion(data, criterion):
            return False

    return True
