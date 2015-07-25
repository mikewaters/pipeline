
# import os
# import logging
# #from .celery import app
#
# #from celery.app.base import Celery
# #from celery._state import _get_current_app, _set_current_app

# # SHUT UP EVERYBODY!
# logging.getLogger("github3").setLevel(logging.WARNING)
# logging.getLogger("requests").setLevel(logging.WARNING)

from contextlib import contextmanager

#from celery.tests.case import UnitApp
# @contextmanager
# def using_test_app(name=None):
#     """Context manager to allow a temporary app to be the current app,
#     for only a short time.  This is necessary, since the side effect of *any*
#     test method creating a TestApp is that will become the default app for all
#      subsequent tests.
#      """
#     #TODO: need to figure out how to get the name of the caller
#     # in order to create a fresh app
#     orig_app = _get_current_app()
#     orig_tasks = []
#     try:
#         app = UnitApp(name=name)
#         orig_tasks = list(app.tasks.keys())
#         yield app
#     finally:
#         # purge all tasks from the app so it can be cleanly reused
#         _set_current_app(orig_app)
        # if not app:
        #     return
        # cur_tasks = list(app.tasks.keys())
        # for task in [t for t in cur_tasks if t not in orig_tasks]:
        #     try:
        #         # THIS DOES NT APPEAR TO WORK AT ALL
        #         app.tasks.unregister(task)
        #     except app.tasks.NotRegistered:
        #         pass


# def _TestApp(name=None, broker='memory://', backend='cache+memory://', **kwargs):
#     """Copy of celery.tests.case.UnitApp.
#     Dont want to import it from there b/c it reqiuires nose.
#
#     Cant currently use this in conbimation with CELERY_ALWAYS_EAGER..
#     """
#     app = Celery(name or 'celery.tests',
#                  set_as_current=True,
#                   broker=broker, backend=backend,
#                  **kwargs)
#     from celery.tests.case import CELERY_TEST_CONFIG
#     from copy import deepcopy
#     app.add_defaults(deepcopy(CELERY_TEST_CONFIG))
#     return app