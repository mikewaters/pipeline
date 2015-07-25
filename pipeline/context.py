"""
``pipeline.context``

Build Context

TODO:
    - Move BuildContext results from simple dicts to objects,
    for easy attribute access.
    - Task name is not going to be sufficient for ``update``,
    because the same task may be executed multiple times in a chain.
    Need to somehow apply ``name`` param of action as it was provided
    by the user.
    Ex:
        { "task": "some_class", "name": "my_task_exec_thingy"}
"""
import six
from jinja2 import Environment

from pipeline.eval import safe_eval


class BuildContext(object):
    """Container for build-time data.
    Stores ``pipeline.bases.Source`` object, as well as
    the result for tasks executed in a chain.

    No need to worry about mutable state here, since this
    object will be serialized and deserialized by celery
    between execution of tasks.
    """

    def __init__(self, **kwargs):
        self.kwargs = kwargs

        #TODO this should be some magic in getattr
        for k, v in self.kwargs.items():
            setattr(self, k, v)

        #TODO: read these in getattr
        self.results = {}

    def update(self, action_or_name, result):
        """Update the build context state with the result
        of a task.
        FIXME action_or_name
        """

        if type(action_or_name) == 'TaskAction':
            name = action_or_name.__name__
        else:
            name = action_or_name

        assert not name in self.results.keys(), \
            'multiple task exec not yet supported'

        self.results[name] = result

        return self

    def render(self, template, **kwargs):
        """Render a template using build context data.
        """
        env = Environment()
        env.globals.update(self.eval_context)
        env.globals.update(kwargs)
        return env.from_string(template).render()

    def render_params(self, source, *args, **kwargs):
        """Render all the things!
        Apply rendering to all the args and kwargs that were
        provided to a task.
        Need a better solution to this brute-force copy/paste job.
        """
        for idx, arg in enumerate(args[2:]):
            if isinstance(arg, six.string_types):
                args[idx] = self.render(arg, source=source)
            elif isinstance(arg, (list, tuple)):
                dest = []
                for item in arg:
                    dest.append(self.render(item, source=source))
                args[idx] = dest

        for k, v in kwargs.items():
            if isinstance(v, six.string_types):
                kwargs[k] = self.render(v, source=source)
            elif isinstance(v, (list, tuple)):
                dest = []
                for item in v:
                    dest.append(self.render(item, source=source))
                kwargs[k] = dest

        return args, kwargs

    def evaluate(self, expression):
        """Evaluate an expression using context.
        """
        return safe_eval(expression, self.eval_context)

    @property
    def eval_context(self):
        """Place interesting things in a dict to be sent to eval.
        """
        context = {}

        for k, v in self.results.items():
            context.update({k: v})
        for k, v in self.kwargs.items():
            context.update({k: v})
        return context