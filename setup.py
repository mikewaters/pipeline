
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

from pip.req import parse_requirements

install_reqs = parse_requirements(
    'requirements.txt',
    session=False
)
reqs = [str(ir.req) for ir in install_reqs]

setup(
    name='celery-pipeline',
    version='0.15',
    description="Runtime-configurable execution pipeline built on celery.",
    author='Mike Waters',
    author_email='robert.waters@gmail.com',
    url='https://github.com/mikewaters/pipeline',
    packages=[
        'pipeline',
    ],
    include_package_data=True,
    install_requires=reqs,
    license="BSD",
    keywords='pipeline',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.4',
    ],
)
