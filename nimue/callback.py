# Copyright (c) 2021 James Lucas

"""
Builtin callback functions, and associated support functions.

Healthcheck Callbacks
=====================
Healthcheck for connection liveness is performed by calling a healthcheck callback function. The default
is **healthcheck_callback_std**. There's also an included **healthcheck_callback_oracle** for use with
Oracle. These should suffice for most use-cases, but if something more complex is needed, there are two
options:
* The builtin callbacks use the base function **healthcheck_callback_base**, which accepts query as an
  argument. The user can define their own callback calling this function with a custom query.
* The user can also define a completely custom callback function. It should return False on failure and
  True on success.
A custom healthcheck callback should follow the signature functionname(conn,dbmodule,logger). Where *conn* is
the raw connection object to be be checked, *dbmodule* is the database module the connection belongs to
(useful for exception handling), and *logger* is the Python logging.Logger used by Nimue for logging.
Nimue passes these parameters when calling the callback.
"""

import contextlib

def healthcheck_callback_std(conn,dbmodule,logger):
  """Standard healthcheck callback for SQLite, Postgres, MSSQL, etc. Runs SELECT 1 as the check query."""
  return healthcheck_callback_base(conn,dbmodule,logger,"SELECT 1")

def healthcheck_callback_oracle(conn,dbmodule,logger):
  """Healthcheck callback for Oracle. Runs SELECT 1 FROM DUAL as the check query."""
  return healthcheck_callback_base(conn,dbmodule,logger,"SELECT 1 FROM DUAL")

def healthcheck_callback_base(conn,dbmodule,logger,query):
  """Healthcheck logic the other builtin functions call. Can be used to implement a healthcheck with a custom query."""
  try:
    with contextlib.closing(conn.cursor()) as curs:
      curs.execute(query)
    conn.rollback()
  except dbmodule.OperationalError:
    return False
  except:
    logger.exception('Unexpected exception during healthcheck. Connection will be invalidated.')
  return True
