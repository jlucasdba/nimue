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
import logging

logger = logging.getLogger(__name__)

def healthcheck_callback_std(conn,dbmodule):
  """
  Standard healthcheck callback for SQLite, Postgres, MSSQL, etc. Runs SELECT 1 as the check query.
  :param conn: The raw connection being healthchecked.
  :param dbmodule: The db driver module conn is derived from. Provided for access to driver-specific exceptions.
  """
  return healthcheck_callback_base(conn,dbmodule,"SELECT 1")

def healthcheck_callback_oracle(conn,dbmodule):
  """
  Healthcheck callback for Oracle. Runs SELECT 1 FROM DUAL as the check query.
  :param conn: The raw connection being healthchecked.
  :param dbmodule: The db driver module conn is derived from. Provided for access to driver-specific exceptions.
  """
  return healthcheck_callback_base(conn,dbmodule,"SELECT 1 FROM DUAL")

def healthcheck_callback_base(conn,dbmodule,query):
  """
  Healthcheck logic the other builtin functions call. Can be used to implement a healthcheck with a custom query.
  :param conn: The raw connection being healthchecked.
  :param dbmodule: The db driver module conn is derived from. Provided for access to driver-specific exceptions.
  :param query: Custom healthcheck query.

  This function should be called by a user-defined healthcheck callback function with a custom query. For example::
    def myhealthcheck(conn,dbmodule):
      return healthcheck_callback_base(conn,dbmodule,"select * from mytable")
  """
  try:
    with contextlib.closing(conn.cursor()) as curs:
      curs.execute(query)
      # some drivers don't like closing a cursor with unfetched rows
      curs.fetchall()
    conn.rollback()
  except dbmodule.OperationalError:
    return False
  except:
    logger.exception('Unexpected exception during healthcheck. Connection will be invalidated.')
    return False
  return True
