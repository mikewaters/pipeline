""" Pipeline Actions

Provide infrastructure for creating and scheduling celery tasks.
Action is a container for a task, to provide some syntactic
sugar without messing around with Task class internals.

To register actions, define a task function and a TaskAction subclass that
refers to it, or (preferably) use the @action decorator on the task function
itself.
"""

import logging
import six

from celery import shared_task, Task

from pipeline.workspace import get_workspace

logger = logging.getLogger(__name__)

class ActionRegistry(type):
    """Registry for action storage."""
    def __init__(cls, name, bases, nmspc):
        super(ActionRegistry, cls).__init__(name, bases, nmspc)
        if not hasattr(cls, '_registry'):
            # top-level parent class, do not map to a task
            cls._registry = {}
            return
        if not hasattr(cls, '_name') or not hasattr(cls, '_task'):
            logger.error('cannot register cls {}'.format(cls))
            return
        cls._registry[cls._name] = cls._task  # noqa

    def get(cls, name):
        """Get a registered task function for a given name."""
        try:
            task_func = cls._registry[name]
        except KeyError:
            raise ValueError('{} not found in registry for {}'.format(
                name, cls.__name__
            ))
        else:
            return task_func


class TaskAction(metaclass=ActionRegistry):
    """Base Action class.  Subclasses need to define both
    ``_task`` and ``_name`` class attributes in order to be registered.
    We distinguish between 'task' and 'partial', even though 'partial'
    is derived from 'task'.  Having original task laying around is really
    helpful for testing, as you can make assertions using it that you cannot
    make with a partial.

    TODO: something is wonky here.  TaskAction instances arent really in
    the registry, the tasks underneath them are.
    For instance, I cannot run `TaskAction.get('some_name') and get a TaskAction
    back; what I get is an actual celery task. (the thing that will be in
    `self.task` after `TaskAction.__init__` is run). #FIXME
    Instead, i have to create a TaskAction instance providing the name of
    the task function.

    """

    def __init__(self, task_name, name=None, callbacks=None, workspace=None, workspace_kwargs=None, **task_kwargs):
        self.task = self.__class__.get(task_name)
        self.partial = None
        self.name = name or "{}.{}".format(self.task.__module__, task_name)
        self.callbacks = callbacks or []
        self.workspace = workspace
        self.workspace_kwargs = workspace_kwargs or {}
        self.task_kwargs = task_kwargs or {}

    @property
    def prepared(self):
        """Executor shouldnt run tasks that havent been signatured"""
        return bool(self.partial)

    # def delay(self, *args, **kwargs):
    #     # used for testing; in the real world, we run actions
    #     # using an executor
    #     if self.partial:
    #         return self.partial.delay(*args, **kwargs)
    #     elif self.task:
    #         return self.task.delay(*args, **kwargs)
    #     raise Exception('not ready')  # TODO

    def prepare(self, source, build_context=None):  # **task_kwargs):
        """Return a task ready for a signature.
        Always use a bound task.
        All task_kwargs will be used to generate the partial.

        :param name: the action name in registry
        :returns: a TaskAction containing the task and partial
        """
        task = self.task
        kwargs = {}
        kwargs.update(self.task_kwargs)
        kwargs.update({
            '_pipeline_chain_state': {
                'callbacks': self.callbacks,
                'action_name': self.name,
            }
         })

        # This is normally done in ``pipeline_task_wrapper``, but that
        # is executed during task runtime on the worker.
        # Here we are running on the broker, priming the chain with
        # an initial build context.
        # TODO this responsibility probably belongs in the executor, not here.
        if build_context:
            kwargs['_pipeline_chain_state'].update(
                {'build_context': build_context}
            )

        kwargs.update({'_pipeline_workspace': {
            'klass': self.workspace,
            'kwargs': self.workspace_kwargs
        }})

        partial = task.s(
            source,
            **kwargs
        )

        #TODO: just return the partial instead of hanging off self
        # this will necessitate removal of delay() method, which I think
        # is only used for tests
        # ^^ This will require some plumbing changes - @mike
        #self.partial = partial
        return partial

    @classmethod
    def from_dict(cls, dct):
        """Build action from dictionary.
        Eventually we will pull things out of the dict and add
        them to the model.


        """
        callbacks = []  # list of tuples
        if 'callbacks' in dct:
            for item in dct['callbacks']:
                # because predicate is not a normal task attribute, pop it before serializing
                # default of 'True' will cuase it to always execute.
                predicate = item.pop('predicate', 'True')
                callbacks.append((
                    cls.from_dict(item),
                    predicate
                ))

        workspace_cls = dct.get('workspace')
        workspace_kwargs = dct.get('workspace_kwargs')

        task_action = cls(
            dct['action'],
            name=dct.get('name'),
            callbacks=callbacks,
            workspace=workspace_cls,
            workspace_kwargs=workspace_kwargs,
            **dct.get('kwargs', {})
        )

        return task_action


def register_action(name, task):
    """Convenience function to register a task/action relationship,
    without having to define a TaskAction subclass.
    """
    type('__TempCls', (TaskAction,), {'_name': name, '_task': task})

def pipeline_task_wrapper(f):
    """Small wrapper around celery tasks registered as Actions.
    Updates the build context supplied by pipeline with the results
    of the wrapped task, and then returns that context, which will
    appear, to the worker, to be the return value of the task.
    Coupled with mutable task signatures (a-la ``subtask()``), this
    allows the up-to-date build context to be passed from task to task
    in a celery chain like a truckstop hoo-a.
    """
    def newf(*args, **kwargs):

        assert len(args) > 0, 'task must be bound'  # missing 'self'
        assert isinstance(args[0], Task), 'task must be bound' # `self` must be a Task subclass

        self = args[0]
        source = args[1]

        args, kwargs = self.build_context.render_params(source, *args, **kwargs)
        ret = f(*args, **kwargs)

        # i think this scenario is only relevant for tests,
        #maybe remove it??  TODO
        if not hasattr(self, '_pipeline_chain_state'):
            # not a chained pipeline task
            raise NotImplementedError

        return self._pipeline_chain_state['build_context'].update(
            self._pipeline_chain_state['action_name'],
            ret
        )

    # There are some incompatibilities between celery and functools.wraps
    # with respect to sending wrapped functions as tasks.
    # Because celery relies on __module__.__name__ for task identification, and since
    # we don't want all tasks to have the same name 'pipeline.actions.newf', we must
    # manually modify __name__ and __module__.
    newf.__name__ = f.__name__
    newf.__module__ = f.__module__
    return newf


def action(*args, **kwargs):
    """Register an Action in pipeline.
    An action is a blahblah blah.
    args and kwargs are routed to ``celery.shared_task``,
    and may be used by pipeline as well (see `name``).
    """
    app_wrapper = shared_task
    def wrapper(**_kwargs):
        def __inner(func):
            tsk = app_wrapper(pipeline_task_wrapper(func), **_kwargs)
            register_action(
                # `name` kwarg will be reused for our module
                # registration, as well as for celery task registry
                _kwargs.get('name', func.__name__),
                tsk,
            )
            return tsk
        return __inner

    kwargs['bind'] = True
    kwargs['base'] = PipelineTask
    if len(args) == 1 and callable(args[0]):
        return wrapper(**kwargs)(args[0])
    return wrapper(*args, **kwargs)



class PipelineTask(Task):
    """Celery task wrapper, supports success/failure handler tasks
    and persisting source and parent return value into self.
    ^^ this is easier than relying on celery, since it forces
    task writers to write supporting boilerplate.
    """
    abstract = True

    def __call__(self, *args, **kwargs):
        # allow these exeptions to propagate, since if they werent
        # set it means the action was never prepared, which is a
        # violation of the api
        state = kwargs.pop('_pipeline_chain_state')

        workspace_instructions = kwargs.pop('_pipeline_workspace')

        # this needs to be run on the worker, not the broker
        self._pipeline_workspace = get_workspace(
            workspace_instructions['klass'],
            **workspace_instructions['kwargs']
        )

        self._pipeline_chain_state = state

        if not len(args) and not 'build_context' in self._pipeline_chain_state:
            raise ValueError('something went sideways')

        # store the result of the last-executed task (if one) into self.
        # we need to rely on the system to make sure that a task that
        # comes in with no args (but having a state) truly is the first task in a chain,
        # and it should already have a build context provided to it
        # by the executor.

        if len(args):  # maybe should be `len(args) > 1`, TODO?
            #TODO: i use type().__name__ here to reduce coupling
            # and avoid circular imports,  find a better way to do this.
            if type(args[0]).__name__ == 'BuildContext':
                # means we are a non-zeroth element in a chain,
                # and the pipeline task wraper has leveraged mutable
                # signatures to pass the build context to us.
                self._pipeline_chain_state['build_context'] = args[0]
                args = args[1:]

        #REMOVED, because who cares?
        # `source` should be the zeroth non-key arg
        #assert len(args) == 1, 'passing non key args to actions is not supported yet'

        return super(PipelineTask, self).__call__(*args, **kwargs)

    @property
    def build_context(self):
        """We dont want this to be used, but it' good for the transition to new api
        """
        if not hasattr(self, '_pipeline_chain_state'):
            raise AttributeError('task not executed in a pipeline')
        return self._pipeline_chain_state['build_context']

"""TODO: remove everything below this line.
This is the old API
"""
class Result(object):
    def __init__(self, result):
        self.result = result


class TaskResult(object):
    """Base container for a task result.

    Note: It is not required to use this class.
    As long as the thing added to context can be evaluated as a boolean,
    and it uses attribute access.

    Since we wish to chain arbitrary tasks, passing the return value
    of one task to the invocation of another, it is hepful for each
    task to share a common return signature.
    """
    def __init__(self, return_value, **kwargs):
        assert isinstance(return_value, bool)

        self.return_value = return_value
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __repr__(self):
        """This is required for celery 3.1, some internals
        use repr during serialization."""
        return "TaskResult({}, output='{}')".format(
            str(self.return_value),
            self.output
        )

    def __str__(self):
        return str(self.return_value)

    def __bool__(self):
        """Task return values should always implement this,'
        so a test for handler execution will work properly.
        """
        return self.return_value

#
# class TemplatedTaskMixin(object):
#     """Templated Task subclass.
#     Provides a ``render()`` method that a task can use to render
#     a predefined template using data from the provided source
#     (or elsewhere, I suppose).
#     # NOTE: celery Task class does not behave properly with respect
#     # to mro for __call__() method.  It is not possible to use
#     # multiple inheritance with chaining of __call__() for Task subclasses.
#     # ALL we can do is provide pure mixins, ust adding methods and not
#     # overriding or redefining them.
#     """
#     def __call__(self, retval=None, source=None, template=None, **kwargs):
#         # stick child task list into a private instance var
#         assert template
#         self._pipeline_render_context = {
#             'source': source,
#             'parent': retval
#         }
#         self._pipeline_template = template
#         #self._pipeline_render_retval = retval
#         #self._pipeline_render_source = source
#
#         return super(TemplatedTaskMixin, self).__call__(retval, source, **kwargs)
#     def render(self):
#         """Render template wth context."""
#         # render_context = {
#         #     'source': self.source,
#         #     'parent': self.parent_retval
#         # }
#         env = Environment()
#         env.globals.update(self._pipeline_render_context)
#         return env.from_string(self._pipeline_template).render()
