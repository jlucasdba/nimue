# Copyright (c) 2021 James Lucas

"""
Database connection pooler for Python.

For basic usage, import the main package with::

  import nimue

Optionally, import submodules with additional functionality if needed::

  import nimue.callback

Public Classes For Direct Use
=============================
* ``NimueConnnectionPool`` The main connection pool class. Initializes a database connection pool.

Other Public Classes
====================
* ``NimueConnection`` Returned by getconnection(). Wraps a raw database connection, and returns it to the pool when closed.
* ``NimueConnectionPoolStats`` Returned by the pool getstats() method. Contains fields describing pool runtime statistics, suitable for monitoring.

See nimue.nimue for the detailed API.
"""

from nimue.nimue import *
