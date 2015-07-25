=============================
Pipeline
=============================

.. image:: https://badge.fury.io/py/pipeline.png
    :target: https://badge.fury.io/py/pipeline

.. image:: https://travis-ci.org/mikewaters/pipeline.png?branch=master
    :target: https://travis-ci.org/mikewaters/pipeline

.. image:: https://coveralls.io/repos/mikewaters/pipeline/badge.png?branch=master
    :target: https://coveralls.io/r/mikewaters/pipeline?branch=master

Execution pipeline built on celery.

Documentation
-------------

The full documentation is at https://pipeline.readthedocs.org.

Quickstart
----------

Install Pipeline::

    pip install pipeline

Then use it in a project::

    import pipeline

Tests
-----
All the tests fail due to a major refactor.  But if you like red console test, run this:

    mkvirtualenv -p `which python3` pipeline
    tox


