=============================
Pipeline
=============================

Execution pipeline built on celery.
`pipeline` is a library to facilitate the execution of shell scripts
and python functions, configured at runtime.


Quickstart
----------

Install Pipeline::

    pip install pipeline

Then use it in a project::

    import pipeline

Tests
-----
All the tests fail due to a major refactor.
But if you like pretty red console text, run this:

    mkvirtualenv -p `which python3` pipeline
    tox

