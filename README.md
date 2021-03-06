# Nimue

*Strange women lying in ponds distributing swords is no basis for a system of government! --Dennis, Peasant*

Nimue is a database connection pool for Python. It aims to work with any thread-safe [DBAPI 2.0](https://www.python.org/dev/peps/pep-0249/) compliant SQL database driver. It is written in pure Python, and supports Python 3.3+. Connections in the pool are periodically checked for liveness by a background thread, and replaced if needed.

Here's a fairly trivial example using [Bottle](https://bottlepy.org/):

```python
from bottle import route, run
from contextlib import closing
from nimue import NimueConnectionPool
import psycopg2

def connfunc():
  return psycopg2.connect(user='postgres')

@route('/')
def index():
  tables={'tables': list()}

  # contextlib context manager wrapping conn, closes conn on exit
  with closing(pool.getconnection()) as conn:
    # psycopg2 Connection's native context manager - wraps a transaction, commits on exit
    with conn:
      # cursor context manager, closes cursor on exit
      with conn.cursor() as curs:
        curs.execute("select tablename from pg_tables order by tablename")
        for r in curs:
          tables['tables'].append(r[0])
  return tables

# context manager for pool - closes pool on exit
with NimueConnectionPool(connfunc,poolmin=10,poolmax=20) as pool:
  run(host='localhost', port=8080)
```

This is a fully functioning web app, returning a json document with a list of tables in a postgres database. One big thing to note is the use of context managers throughout the example. It's important not to leak connections from the pool. If a Connection object goes out of scope, it *should* end up back in the pool when garbage collected, but this shouldn't be counted upon. Context managers provide a reliable way to make sure objects are released as soon as they pass out of scope - even in the event of something unexpected like an unhandled exception.

Although not specified by PEP 249, all the major database drivers use Connection's context manager to wrap a transaction (as seen in the example), rather than the open/close cycle of a connection. This might not be the behavior you expect. Fortunately contextlib provides the closing() function, which provides a context manager that calls close() on exit. Using it is strongly encouraged.

### Status
Nimue is currently in a pre-release state. Core functionality should all work, and the interface will probably remain mostly stable, but no promises until 1.0. In particular, all errors raise raw Exception right now - custom exception classes remain to be added.

Also worth noting so far Nimue has mostly been tested against sqlite3 and psycopg2 drivers. Limited testing has been done with all the drivers listed in the testsuite. There shouldn't be major issues with any DBAPI compliant drivers.

### Driver-Specific Notes
#### sqlite3
The sqlite3.connect() keyword arg check_same_thread must be set to False. Otherwise multi-threaded access won't be allowed for connections.

#### Oracle
The healthcheck_callback should be set to nimue.callback.healthcheck_callback_oracle (or an appropriate custom healthcheck). The default healthcheck callback (nimue.callback.healthcheck_callback_std) won't work on Oracle.

#### TODO
- Full documentation
- SQLAlchemy interoperability
- Custom exception classes

### Installation

Nimue can be installed with pip. You can also download from the releases page if you're feeling adventurous.

```bash
pip install nimue
```

### Documentation
The module is documented through docstrings - currently there's no online API documentation, but you can use a tool like pydoc or pdoc3 to generate your own.

Copyright (c) 2021-2022 James Lucas
