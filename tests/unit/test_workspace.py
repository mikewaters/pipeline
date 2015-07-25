import os
from tempfile import TemporaryDirectory
from pipeline.workspace import workspace


def test_workspace_location_create_delete():
    """Test that a workspace location is correct, and that it is
    created and deleted correctly."""
    with TemporaryDirectory() as wdir:
        with workspace(
            name='blerg',
            basepath=wdir,
            hints=[1, 2, 'a'],
            reusable=True,
            delete=True
        ) as w:
            loc = w.location
            assert os.path.exists(loc)
            expected = os.path.join(
                wdir, "workspace-blerg-1-2-a"
            )
            assert loc == expected

        assert not os.path.exists(loc)
