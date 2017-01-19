# Copyright (c) 2011-2013 by Enthought, Inc.
# All rights reserved.

import os.path
from setuptools import setup, find_packages
import sys

encore_init = os.path.join('encore', '__init__.py')
if sys.version_info.major == 3:
    import runpy
    d = runpy.run_path(encore_init)
else:
    d = {}
    execfile(encore_init, d)

setup(
    name='encore',
    version=d['__version__'],
    author='Enthought, Inc',
    author_email='info@enthought.com',
    url='http://docs.enthought.com/encore/',
    download_url='https://github.com/enthought/encore',
    description='Low-level core modules for building Python applications',
    long_description=open('README.rst').read(),
    packages=find_packages(),
    requires=[],
)
