import contextlib
import datetime
import logging
import sys
import threading
import time

logger = logging.getLogger(__name__)

class NimueCleanupThread(threading.Thread):
  def __init__(self,owner):
    super().__init__()
    self.exitevent=owner._exitevent
    self.owner=owner

  def run(self):
    while(not self.exitevent.wait(timeout=60)):
      self.owner._healthcheckpool()

class NimueConnectionPool(object):
  def __init__(self,dbmodule,connargs=None,connkwargs=None,initial=10,max=20):
    self.dbmodule=dbmodule
    self.connargs=connargs
    if self.connargs is None:
      self.connargs=[]
    self.connkwargs=connkwargs
    if self.connkwargs is None:
      self.connkwargs={}
    self.initial=initial
    self.max=max

    self._pool={}
    self._free=[]
    self._use={}
    self._lock=threading.Condition()

    while len(self._pool) < self.initial:
      self._addconnection()

    self._exitevent=threading.Event()
    self._healthcheckthread=NimueCleanupThread(self)
    self._healthcheckthread.start()

  def __del__(self):
    self.close()

  def __enter__(self):
    return self

  def __exit__(self,exc_type,exc_value,traceback):
    self.close()

  def _addconnection(self):
    member=NimueConnectionPoolMember(dbmodule=self.dbmodule,conn=self.dbmodule.connect(*self.connargs,**self.connkwargs))
    with self._lock:
      self._pool[member]=1
      self._free.insert(0,member)
      self._lock.notify()

  def _healthcheckpool(self):
    with self._lock:
      dead=[]
      for x in enumerate(self._free):
        if not x[1].healthcheck():
          dead.append(x[0])
      for x in sorted(dead,reverse=True):
        del self._free[x]
        logger.warn("Closing dead connection in slot %d" % x)
      for x in dead:
        self._addconnection()

  def getconnection(self,blocking=True,timeout=None):
    if timeout is not None and timeout < 0:
      raise Exception("Timeout must be 0 or greater")
    if timeout is None:
      timeout=-1  # this is the value Lock expects

    # Capture the time we entered the function
    entertime=time.monotonic()
    r=self._lock.acquire(blocking,timeout)
    # If we weren't able to acquire the lock in given timeframe, give up
    if not r:
      return None

    # At this point we have the lock - the context manager ensures we release it
    with contextlib.ExitStack() as stack:
      stack.push(self._lock)
      # if there's a free connection in the pool, we are good
      if len(self._free) > 0:
        member=self._free.pop(0)
        self._use[member]=1
        return NimueConnection(self,member)
      # if there's room to add a new connection, we are also good
      elif len(self._pool.keys()) < self.max:
        member=self._addconnection()
        member=self._free.pop(0)
        self._use[member]=1
        return NimueConnection(self,member)
      # but if neither of those are true, now we have to wait
      # (or give up if blocking is False)
      else:
        if not blocking:
          return None
        r=self._lock.wait_for(len(self._free) > 0,timeout-(time.monotonic()-entertime))
        if not r:
          return None
        member=self._free.pop(0)
        self._use[member]=1
        return NimueConnection(self,member)

  def close(self):
    for x in self._pool.keys():
      x.conn.close() 
    self._exitevent.set()
    self._healthcheckthread.join()

class NimueConnectionPoolMember(object):
  def __init__(self,dbmodule,conn):
    self.dbmodule=dbmodule
    self.conn=conn
    self.create_time=datetime.datetime.now()
    self.touch_time=self.create_time
    self.check_time=self.create_time

  def healthcheck(self):
    r=True
    try:
      curs=self.conn.cursor()
      curs.execute("select 1")
      self.conn.rollback()
      curs.close()
    except self.dbmodule.OperationalError:
      r=False
    except:
      r=False
      logger.exception('Unexpected exception during healthcheck. Connection will be invalidated.')
    self.check_time=datetime.datetime.now()
    return r

  def touch(self):
    self.touch_time=datetime.datetime.now()

class NimueConnection(object):
  def __init__(self,pool,member):
    self._member=member
    self._pool=pool
    self._closed=False
    self._conn=member.conn

  def __getattr__(self,attr):
    if attr in self.__dict__:
      return getattr(self, attr)
    return getattr(self._conn,attr)

  def __setattr__(self,attr,value):
    if attr in ('_pool','_conn','_member','_closed'):
      self.__dict__[attr]=value
    else:
      self._conn.__dict__[attr]=value

  def __enter__(self):
    self._conn.__enter__()
    return self

  def __exit__(self,exc_type,exc_value,traceback):
    self._conn.__exit__(exc_type,exc_value,traceback)

  def close(self):
    # if _closed is True, we do nothing and return
    if self._closed:
      return

    self._conn.rollback()
    with self._pool._lock:
      del self._pool._use[self._member]
      self._pool._free.insert(0,self._member)
      self._pool._lock.notify()
    # explicitly remove references to other objects
    del self._member
    del self._pool
    del self._conn
    self._closed=True