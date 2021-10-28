# Copyright (c) 2021 James Lucas

import contextlib
import logging
import importlib
import threading
import time

logger = logging.getLogger(__name__)

class _NimueSubCleanupThread(threading.Thread):
  def __init__(self,owner):
    super().__init__(daemon=False)
    self.owner=owner

  def run(self):
    self.owner._healthcheckpool()

class _NimueCleanupThread(threading.Thread):
  def __init__(self,owner):
    super().__init__(daemon=True)
    self.exitevent=owner._exitevent
    self.owner=owner

  def run(self):
    while not self.exitevent.wait(timeout=self.owner.cleanup_interval):
      subthread=_NimueSubCleanupThread(self.owner)
      subthread.start()
      subthread.join()

class NimueConnectionPool:
  def __init__(self,connfunc,connargs=None,connkwargs=None,poolinit=None,poolmin=10,poolmax=20,cleanup_interval=60,idle_timeout=300):
    if poolmin < 0:
      raise Exception("Value for poolmin cannot be less than 0")
    if poolinit is not None and  poolinit < poolmin:
      raise Exception("Value for poolinit cannot be less than poolmin during initialization")
    if poolmax < 1:
      raise Exception("Value for poolmax cannot be less than 1")
    if poolinit is not None and poolmax < poolinit:
      raise Exception("Value for poolmax cannot be less than value for poolinit during initialization")
    if poolmax < poolmin:
      raise Exception("Value for poolmax cannot be less than value for poolmin")
    if cleanup_interval <= 0:
      raise Exception("Value for cleanup_interval must be greater than 0")
    if idle_timeout < 0:
      raise Exception("Value for idle_timeout cannot be less than 0")

    self._connfunc=connfunc
    self._connargs=connargs
    if self._connargs is None:
      self._connargs=[]
    self._connkwargs=connkwargs
    if self._connkwargs is None:
      self._connkwargs={}
    self._poolinit=poolinit
    self._poolmin=poolmin
    self._poolmax=poolmax
    self._cleanup_interval=cleanup_interval
    self._idle_timeout=idle_timeout

    self._pool={}
    self._free=[]
    self._use={}
    self._lock=threading.Condition()

    # stats counters
    self._connections_cleaned_dead=0
    self._connections_cleaned_idle=0
    self._cleanup_cycles=0

    if self._poolinit is None:
      initial=self._poolmin
    else:
      initial=self._poolinit
    while len(self._pool) < initial:
      self._addconnection()

    self._dbmodule=self._finddbmodule()

    self._exitevent=threading.Event()
    self._healthcheckthread=_NimueCleanupThread(self)
    self._healthcheckthread.start()

  def __del__(self):
    self.close()

  def __enter__(self):
    return self

  def __exit__(self,exc_type,exc_value,traceback):
    self.close()

  @property
  def connfunc(self):
    return self._connfunc

  @property
  def connargs(self):
    return self._connargs

  @property
  def connkwargs(self):
    return self._connkwargs

  @property
  def poolinit(self):
    with self._lock:
      return self._poolinit

  @property
  def poolmin(self):
    with self._lock:
      return self._poolmin

  @poolmin.setter
  def poolmin(self,val):
    if val < 0:
      raise Exception("Value for poolmin cannot be less than 0")
    if val > self.poolmax:
      raise Exception("Value for poolmin cannot be greater than value for poolmax")
    with self._lock:
      self._poolmin=val

  @property
  def poolmax(self):
    with self._lock:
      return self._poolmax

  @poolmax.setter
  def poolmax(self,val):
    if val < 1:
      raise Exception("Value for poolmax cannot be less than 1")
    if val < self.poolmin:
      raise Exception("Value for poolmax cannot be less than value for poolmin")
    with self._lock:
      self._poolmax=val

  @property
  def cleanup_interval(self):
    with self._lock:
      return self._cleanup_interval

  @cleanup_interval.setter
  def cleanup_interval(self,val):
    if val <= 0:
      raise Exception("Value for cleanup_interval must be greater than 0")
    with self._lock:
      self._cleanup_interval=val

  @property
  def idle_timeout(self):
    with self._lock:
      return self._idle_timeout

  @idle_timeout.setter
  def idle_timeout(self,val):
    if val < 0:
      raise Exception("Value for idle_timeout cannot be less than 0")
    with self._lock:
      self._idle_timeout=val

  def _addconnection(self):
    member=_NimueConnectionPoolMember(self,conn=self._connfunc(*self._connargs,**self._connkwargs))
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
        self._connections_cleaned_dead+=1

      # figure out the maximum idle connections we can remove
      idletarget=len(self._pool)-self._poolmin
      if len(self._free) < idletarget:
        idletarget=len(self._free)

      # if we can possibly remove any connections, proceed
      if idletarget > 0:
        # identify idle connections that are removal candidates
        now=time.monotonic()
        for x in enumerate(self._free):
          if now-x[1]._touch_time > self._idle_timeout:
            idle.append(x[0])

        # remove oldest idle connections up to idletarget
        for x in sorted(sorted(idle,key=lambda z: (now-self._free[z]._touch_time, self._free[z]._create_time), reverse=True)[0:idletarget],reverse=True):
          try:
            self._free[x].close()
          except:
            pass
          del self._pool[self._free[x]]
          del self._free[x]
          self._connections_cleaned_idle+=1

      # After removing dead and idle connections, if pool size is in excess of poolmax (because poolmax may have been decreased)
      # remove oldest free connections to try to get back under poolmax. If there aren't enough free connections, we
      # may still be in excess of poolmax though.
      poolmax_excess=len(self._pool) - self._poolmax
      if poolmax_excess > 0:
        if len(self._free) >= poolmax_excess:
          removenum=poolmax_excess
        else:
          removenum=len(self._free)
        for x in sorted(sorted(enumerate(self._free),key=lambda z: (now-z[1]._touch_time, z[1]._create_time), reverse=True)[0:poolmax_excess],key=lambda z: z[0],reverse=True):
          del self._pool[x[1]]
          del self._free[x[0]]

      # add connections till we get back up to _poolmin
      addtarget=self._poolmin - len(self._pool)
      if addtarget > 0:
        for x in range(0,addtarget):
          self._addconnection()
    self._cleanup_cycles+=1

  def _finddbmodule(self):
    with self._lock:
      if len(self._free) == 0:
        raise Exception("No available connections to examine for determining dbmodule")
      modulename=self._free[0]._conn.__class__.__module__
      components=modulename.split('.')
      while len(components) > 0:
        dbmodule=importlib.import_module('.'.join(components))
        if 'connect' in dir(dbmodule):
          return dbmodule
        del components[-1]
      raise Exception("Could not determine main dbmodule")

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
      elif len(self._pool.keys()) < self._poolmax:
        self._addconnection()
        member=self._free.pop(0)
        self._use[member]=1
        return NimueConnection(self,member)
      # but if neither of those are true, now we have to wait
      # (or give up if blocking is False)
      else:
        if not blocking:
          return None
        # if timeout is -1, block indefinitely, else block for the remaining time
        if timeout == -1:
          r=self._lock.wait_for(lambda: len(self._free) > 0,None)
        else:
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
      return NimueConnectionPoolStats(poolsize,poolused,poolfree,self._connections_cleaned_dead,self._connections_cleaned_idle,self._cleanup_cycles)

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
        logger.debug("Size of _pool is: %d", len(self._pool))
        logger.debug("Size of _free is: %d", len(self._free))
        logger.debug("Size of _use is: %d", len(self._use))
        self._lock.wait_for(lambda: len(self._free) > 0)
        for x in sorted(enumerate(self._free),key=lambda z: z[0],reverse=True):
          x[1].close()
          del self._pool[x[1]]
          del self._free[x[0]]

class _NimueConnectionPoolMember:
  def __init__(self,owner,conn):
    self._owner=owner
    self._conn=conn
    self._create_time=time.monotonic()
    self._touch_time=self._create_time
    self._check_time=self._create_time

  def healthcheck(self):
    r=True
    with self._owner._lock:
      operr=self._owner._dbmodule.OperationalError
    try:
      curs=self._conn.cursor()
      curs.execute("select 1")
      self._conn.rollback()
      curs.close()
    except operr:
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

class NimueConnection:
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

class NimueConnectionPoolStats:
  def __init__(self,poolsize,poolused,poolfree,connections_cleaned_dead,connections_cleaned_idle,cleanup_cycles):
    self.poolsize=poolsize
    self.poolused=poolused
    self.poolfree=poolfree
    self.connections_cleaned_dead=connections_cleaned_dead
    self.connections_cleaned_idle=connections_cleaned_idle
    self.cleanup_cycles=cleanup_cycles
