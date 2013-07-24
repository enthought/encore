# Copyright (c) 2011-2013 by Enthought, Inc.
# All rights reserved.

import os.path
from setuptools import setup, find_packages

d = {}
execfile(os.path.join('encore', '__init__.py'), d)

setup(
    name='encore',
    version=d['__version__'],
    author='Enthought, Inc',
    author_email='info@enthought.com',
    url='https://github.com/enthought/encore',
    description='Low-level core modules for building Python applications',
    long_description=open('README.rst').read(),
    packages=find_packages(exclude=('*.tests',)),
    requires=[],
)
