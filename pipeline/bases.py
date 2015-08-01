
__all__ = ['Registry']

class Registry(type):
    """Simple implementation of the Registry pattern using the
    declarative style.

    Classes that wish to implement a registry should utilize this as
    their metaclass.

    Subclasses of registries ("registered classes" should define the
    class attribute `__id`, which will serve as their key in the registry.

    Subclasses of registries that do not define an `__id` are considered
    'abstract', and will not be added to the registry.
    Their purpose should only be to serve to facilitate OOP patterns.
    """
    def __init__(cls, name, bases, nmspc):
        super(Registry, cls).__init__(name, bases, nmspc)
        if not hasattr(cls, '_registry'):
            # assume base type here (superclass), which has no instances,
            # only implementations.
            cls._registry = {}
            return
        _id = '_{}__id'.format(name)
        if hasattr(cls, _id):
            cls._registry[getattr(cls, _id)] = cls  # noqa
            pass

    def factory(cls, name, *args, **kwargs):
        """Return an instance of the registered Thing identified by ``name``."""
        klass = cls.get(name)
        if not klass:
            raise KeyError('{} having name {} not found'.format(cls, name))
        return klass(*args, **kwargs)

    def get(cls, name):
        """Return a registred subclass by it's __id."""
        return cls._registry.get(name)

    def getlist(cls):
        """Return all the subclasses registered for a given class.
        """
        return filter(
            lambda k: issubclass(k, cls),
            cls._registry.values()
        )

    def find(cls, attr, value):
        """Find registered subclasses that define the attribute `attr`
        which has the value ``value``.
        """
        available = cls.getlist()
        found = []
        for v in available:
            if hasattr(v, attr) and getattr(v, attr) == value:
                found.append(v)
        return found
