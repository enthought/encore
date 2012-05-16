# Copyright (c) 2011-2012 by Enthought, Inc.
# All rights reserved.

from setuptools import setup, find_packages


setup(
    name='encore',
    version='0.2',
    author='Enthought, Inc',
    author_email='info@enthought.com',
    url='https://github.com/enthought/encore',
    description='Low-level core modules for building Python applications',
    long_description=open('README.rst').read(),
    packages=find_packages(exclude=('*.tests',)),
    requires=[],
)
