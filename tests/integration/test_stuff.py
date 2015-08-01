from pipeline.actions import TaskAction
from pipeline import Pipeline

import logging
logger = logging.getLogger(__name__)



def test_single_action():
    """Test a single action scheduled by the executor.
    """
    actions = [
        TaskAction(
            'stuff_increment_source',
            name= 'increment',
            amount= 1

        ),
    ]
    executor = Pipeline(actions)
    result = executor.schedule(1).get()
    assert result.results['increment'] == 2