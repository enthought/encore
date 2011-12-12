#------------------------------------------------------------------------------
#  Copyright (c) 2011, Enthought, Inc.
#  All rights reserved.
#------------------------------------------------------------------------------
# Copyright (c) 2011 by Enthought, Inc.
# All rights reserved.

from setuptools import setup, find_packages

setup(name='encore',
    version='0.1',
    author='Enthought, Inc',
    author_email='info@enthought.com',
    url='https://github.com/enthought/encore',
    description='Low-level core modules for building Python applications',
    long_description=open('README.rst').read(),
    packages=["encore", "encore/events", "encore/storage"],
    requires=[],
    install_requires=['distribute'],
)
