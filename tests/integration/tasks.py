import operator
from pipeline.actions import action

@action
def return_one(self, source):
    return 1

@action
def stuff_increment_source(self, source, amount):
    """Print some stuff to the console."""
    return operator.add(source, amount)