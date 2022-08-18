# Copyright (c) 2011-2022 by Enthought, Inc.
# All rights reserved.

import os.path
import runpy
from setuptools import setup, find_packages

encore_init = os.path.join('encore', '__init__.py')
d = runpy.run_path(encore_init)

def get_long_description():
    """ Read long description from README.txt. """
    with open("README.rst", "r", encoding="utf-8") as readme:
        return readme.read()

setup(
    name='encore',
    version=d['__version__'],
    author='Enthought, Inc',
    author_email='info@enthought.com',
    url='http://docs.enthought.com/encore/',
    download_url='https://github.com/enthought/encore',
    description='Low-level core modules for building Python applications',
    long_description=get_long_description(),
    long_description_content_type="text/x-rst",
    packages=find_packages(),
    requires=[],
    python_requires=">=3.6",
)
