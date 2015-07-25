"""
conftest.py

py.test hook file
"""

from celery import Celery

class Config:
    CELERY_ALWAYS_EAGER = True

app = Celery()
app.config_from_object(Config)
