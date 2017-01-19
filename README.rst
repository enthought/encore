====================================================
EnCore - A collection of core-level utility modules
====================================================

.. image:: https://api.travis-ci.org/enthought/encore.png?branch=master
   :target: https://travis-ci.org/enthought/encore
   :alt: Build status

.. image:: https://coveralls.io/repos/enthought/encore/badge.png?branch=master
   :target: https://coveralls.io/r/enthought/encore
   :alt: Coverage status

This package consists of a collection of core utility packages useful for
building Python applications.  This package is intended to be at the
bottom of the software stack and have zero required external dependencies
aside from the Python Standard Library.

Packages
--------

*Events:* A package implementing a lightweight application-wide Event dispatch system.  Listeners
can subscribe to events based on Event type or by filtering on event attributes.  Typical uses
include UI components listening to low-level progress notifications and change notification for
distributed resources.

*Storage:* Abstract base classes and concrete implementations of a basic key-value storage API.
The API is intended to be general purpose enough to support a variety of local and remote storage
systems.

*Concurrent:* A package of tools for handling concurrency within applications.

*Terminal:* Some utilities for working with text-based terminal displays.

Prerequisites
-------------

* Python >= 2.7 or Python >= 3.4

* Sphinx, graphviz, pydot (documentation build)

* Some optional modules have dependencies on:

  - Requests (http://docs.python-requests.org/en/latest/)

  - Futures (https://code.google.com/p/pythonfutures/)


.. |build_status| image:: https://travis-ci.org/enthought/encore.png
