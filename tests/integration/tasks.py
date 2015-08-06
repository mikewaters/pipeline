import operator
from pipeline.actions import action

@action
def return_one(self, source):
    return 1

@action
def stuff_increment_source(self, source, amount):
    """Print some stuff to the console."""
    return operator.add(source, int(amount))

@action
def increment(self, source, num='1', by='1'):
    """An action that increments a value.
    """
    return int(num) + int(by)

@action(call_count=0)
def increment_call_count(self, source):
    self.call_count += 1

@action(called=False)
def named_action(self, source):
    self.called = True
    return True