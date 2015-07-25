import random


def rand_suffix():
    """Generate a workspace suffix.
    :returns: random 8-character hex string
    """
    return '%08x' % random.randrange(16**8)

