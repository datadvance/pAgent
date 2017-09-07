#!/usr/bin/env python

import os
import setuptools

with open(os.path.join(os.path.dirname(__file__), 'README.rst')) as readme:
    README = readme.read()

# Allow `setup.py` to be run from any path.
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setuptools.setup(
    # Main information.
    name='pAgent',
    description='Remotely controller job manager with embedded http proxy.',
    long_description=README,
    version='0.5.0',
    url='https://github.com/datadvance/pAgent',

    # Author details.
    author='DATADVANCE',
    author_email='info@datadvance.net',
    license='MIT License',

    # PyPI classifiers: https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Framework :: AsyncIO',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6'
        'Topic :: Internet :: Proxy Servers',
        'Topic :: Software Development',
    ],

    # Dependencies required to make package function properly.
    packages=setuptools.find_packages(exclude=['test', 'doc']),
    install_requires=[
        'pRpc>=1.0.0',
        'aiohttp>=2.2',
        'jsonschema',
        'psutil',
        'pyyaml',
    ],

    # Test dependencies and settings to run `python setup.py test`.
    tests_require=[
        'pytest',
        'pytest-catchlog',
        'pytest-pythonpath',
    ],
    # Use `pytest-runner` to integrate `pytest` with `setuptools` as it is
    # described in the "Good Integration Practices" chapter in the pytest docs:
    # https://docs.pytest.org/en/latest/goodpractices.html
    setup_requires=[
        'pytest-runner',
    ],
)
