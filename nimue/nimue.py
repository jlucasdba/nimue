import contextlib
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
    while(not self.exitevent.wait(timeout=self.owner.cleanup_interval)):
      self.owner._healthcheckpool()

class NimueConnectionPool(object):
  def __init__(self,dbmodule,connargs=None,connkwargs=None,initial=10,max=20,cleanup_interval=60):
    self._dbmodule=dbmodule
    self._connargs=connargs
    if self._connargs is None:
      self._connargs=[]
    self._connkwargs=connkwargs
    if self._connkwargs is None:
      self._connkwargs={}
    self._initial=initial
    self._max=max
    self._cleanup_interval=cleanup_interval

    self._pool={}
    self._free=[]
    self._use={}
    self._lock=threading.Condition()

    while len(self._pool) < self._initial:
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

  @property
  def dbmodule(self):
    return self._dbmodule

  @property
  def connargs(self):
    return self._connargs

  @property
  def connkwargs(self):
    return self._connkwargs

  @property
  def initial(self):
    with self._lock:
      return self._initial

  @initial.setter
  def initial(self,val):
    with self._lock:
      self._initial=val

  @property
  def max(self):
    with self._lock:
      return self._max

  @max.setter
  def max(self,val):
    with self._lock:
      self._max=val

  @property
  def cleanup_interval(self):
    with self._lock:
      return self._cleanup_interval

  @cleanup_interval.setter
  def cleanup_interval(self,val):
    with self._lock:
      self._cleanup_interval=val

  def _addconnection(self):
    member=NimueConnectionPoolMember(dbmodule=self._dbmodule,conn=self._dbmodule.connect(*self._connargs,**self._connkwargs))
    with self._lock:
      self._pool[member]=1
      self._free.insert(0,member)
      self._lock.notify()

  def _healthcheckpool(self):
    with self._lock:
      idle=[]
      dead=[]

      # identify and clear dead connections
      for x in enumerate(self._free):
        if not x[1].healthcheck():
          dead.append(x[0])
      for x in sorted(dead,reverse=True):
        try:
          self._free[x].close()
        except:
          pass
        del self._pool[self._free[x]]
        del self._free[x]
        logger.warn("Closing dead connection in slot %d" % x)

      # figure out the maximum idle connections we can remove
      idletarget=len(self._pool)-self._initial
      if len(self._free) < idletarget:
        idletarget=len(self._free)

      # if we can possibly remove any connections, proceed
      if idletarget > 0:
        # identify idle connections that are removal candidates
        now=time.monotonic()
        for x in enumerate(self._free):
          if now-x[1].touch_time > 10:
            idle.append(x[0])

        # remove oldest idle connections up to idletarget
        for x in sorted(sorted(idle,key=lambda z: (self._free[z].touch_time, self._free[z].create_time), reverse=True)[0:idletarget],reverse=True):
          try:
            self._free[x].close()
          except:
            pass
          del self._pool[free[x]]
          del self._free[x]

      # add connections till we get back up to _initial
      addtarget=self._initial - len(self._pool)
      if addtarget > 0:
        for x in range(0,addtarget):
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
      # if _exitevent has been set, the pool is being torn down, so raise an exception
      if self._exitevent.is_set():
        raise Exception("Pool %s has already been closed." % self)
      # if there's a free connection in the pool, we are good
      elif len(self._free) > 0:
        member=self._free.pop(0)
        self._use[member]=1
        return NimueConnection(self,member)
      # if there's room to add a new connection, we are also good
      elif len(self._pool.keys()) < self._max:
        member=self._addconnection()
        member=self._free.pop(0)
        self._use[member]=1
        return NimueConnection(self,member)
      # but if neither of those are true, now we have to wait
      # (or give up if blocking is False)
      else:
        if not blocking:
          return None
        r=self._lock.wait_for(lambda: len(self._free) > 0,timeout-(time.monotonic()-entertime))
        if not r:
          return None
        member=self._free.pop(0)
        self._use[member]=1
        return NimueConnection(self,member)

  def poolstats(self):
    with self._lock:
      poolsize=len(self._pool)
      poolused=len(self._use)
      poolfree=len(self._free)
      return NimueConnectionPoolStats(poolsize,poolused,poolfree)

  def close(self):
    # shutdown the cleanup thread
    self._exitevent.set()
    self._healthcheckthread.join()

    with self._lock:
      for x in sorted(enumerate(self._free),key=lambda z: z[0],reverse=True):
        x[1].close()
        del self._pool[x[1]]
        del self._free[x[0]]

      while len(self._use) > 0:
        logger.debug("Size of _pool is: %d" % len(self._pool))
        logger.debug("Size of _free is: %d" % len(self._free))
        logger.debug("Size of _use is: %d" % len(self._use))
        self._lock.wait_for(lambda: len(self._free) > 0)
        for x in sorted(enumerate(self._free),key=lambda z: z[0],reverse=True):
          x[1].close()
          del self._pool[x[1]]
          del self._free[x]

class NimueConnectionPoolMember(object):
  def __init__(self,dbmodule,conn):
    self._dbmodule=dbmodule
    self._conn=conn
    self._create_time=time.monotonic()
    self._touch_time=self._create_time
    self._check_time=self._create_time

  def healthcheck(self):
    r=True
    try:
      curs=self._conn.cursor()
      curs.execute("select 1")
      self._conn.rollback()
      curs.close()
    except self._dbmodule.OperationalError:
      r=False
    except:
      r=False
      logger.exception('Unexpected exception during healthcheck. Connection will be invalidated.')
    self._check_time=time.monotonic()
    return r

  def touch(self):
    self._touch_time=time.monotonic()

  def close(self):
    self._conn.close()

class NimueConnection(object):
  def __init__(self,pool,member):
    self._member=member
    self._pool=pool
    self._closed=False
    self._conn=member._conn

  def __getattr__(self,attr):
    if attr in self.__dict__:
      return getattr(self, attr)
    return getattr(self._conn,attr)

  def __setattr__(self,attr,value):
    if attr in ('_pool','_conn','_member','_closed'):
      self.__dict__[attr]=value
    else:
      self._conn.__dict__[attr]=value

  def __del__(self):
    self.close()

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
      self._member.touch()
      self._pool._lock.notify()
    # explicitly remove references to other objects
    del self._member
    del self._pool
    del self._conn
    self._closed=True

def NimueConnectionPoolStats(object):
  def __init__(self,poolsize,poolused,poolfree):
    self.poolsize=poolsize
    self.poolused=poolused
    self.poolfree=poolfree
