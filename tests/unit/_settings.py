import os
import tempfile

SECRET_KEY = 'mike'
DEBUG = True
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': os.path.join(BASE_DIR, 'pipeline.tests.sqlite3'),
    }
}

BROKER_URL = None  #'redis://localhost:6380/0'

PIPELINE_WORKSPACE_ROOT = tempfile.gettempdir()

INSTALLED_APPS = ['pipeline']
