# Copyright (c) 2021 James Lucas

"""
Submodule implementing core functionality.  Import "nimue" instead of importing this directly.
"""

import contextlib
import logging
import importlib
import threading
import time

from . import callback

logger = logging.getLogger(__name__)

class _NimueSubCleanupThread(threading.Thread):
  def __init__(self,owner):
    super().__init__(daemon=False)
    self.owner=owner

  def run(self):
    self.owner._cleanpool()

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
  """
  Nimue Connection Pool object. An implementation of a database connection pool for DBAPI 2.0 compliant drivers. Allocates
  a configurable number of initial connections, with the ability to increase on-demand up to maximum. Spawns a background
  thread responsible for cleanup. Can be used as a context manager, which calls the close() method upon exit. This is the
  recommended usage. It's important to note that the close() method blocks until all connections are returned to the pool,
  so it is important not to leak connections, and return them when not in use. Use of context managers is strongly encouraged.
  """
  def __init__(self,connfunc,connargs=None,connkwargs=None,poolinit=None,poolmin=10,poolmax=20,cleanup_interval=60,idle_timeout=300,healthcheck_on_getconnection=True,healthcheck_callback=callback.healthcheck_callback_std):
    """
    :param connfunc: (required) A callable that returns a DBAPI 2.0 compliant Connection object.
    :param connargs: Iterable list of args to be passed to connfunc.
    :param connkwargs: Dict containing named keyword arguments to be passed to connfunc.
    :param poolinit: Initial size of the pool. Defaults to None, meaning to use poolmin as the initial size. Cannot be less than poolmin or greater than poolmax.
    :param poolmin: Minimum size of the pool. Defaults to 10. Cannot be less than 0, and cannot be greater than poolmax.
    :param poolmax: Maximum size of the pool. Defaults to 20. Cannot be less than 1, and cannot be less than poolmin.
    :param cleanup_interval: Wakeup interval for cleanup thread, in seconds. Defaults to 60. Must be greater than 0.
    :param idle_timeout: Idle timeout in seconds, after which idle connections are eligible for cleanup. Defaults to 300. Cannot be less than 0.
    :healthcheck_on_getconnection: If True, perform a healthcheck on getconnection from the pool. If the check fails, the connection is discarded from the pool, and the method continues trying connections until a healthy one can be returned (or until timeout occurs, if set). Defaults to True.

    :returns: Returns a NimueConnectionPool object.
    """
    # define these early because otherwise the validation checks
    # will cause cascading exceptions on failure
    self._lock=threading.Condition()
    self._exitevent=threading.Event()
    self._pool={}
    self._free=[]
    self._use={}
    self._cleanupthread=_NimueCleanupThread(self)

    # parameter validations
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
    if not callable(healthcheck_callback):
      raise Exception("Value for healthcheck_callback must be callable")

    # set internal values from parameters
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
    self._healthcheck_on_getconnection=healthcheck_on_getconnection
    self._healthcheck_callback=healthcheck_callback

    # stats counters
    self._connections_cleaned_dead=0
    self._connections_cleaned_idle=0
    self._cleanup_cycles=0

    # initialize the pool
    if self._poolinit is None:
      initial=self._poolmin
    else:
      initial=self._poolinit
    while len(self._pool) < initial:
      self._addconnection()

    # identify the dbmodule used by connections since
    # we need to know it to reference its exceptions
    self._dbmodule=self._finddbmodule()

    # start the healthcheck thread
    self._cleanupthread.start()

  def __del__(self):
    # only call close if at least these attributes have been initialized already
    if len(set(('_lock','_exitevent','_pool','_free','_use','_cleanupthread')) & set(vars(self))) > 0:
      self.close()

  def __enter__(self):
    return self

  def __exit__(self,exc_type,exc_value,traceback):
    self.close()

  @property
  def connfunc(self):
    """Callable used by the pool to produce a new Connection. Read-only."""
    return self._connfunc

  @property
  def connargs(self):
    """Arguments passed to connfunc when it is called. Read-only."""
    return self._connargs

  @property
  def connkwargs(self):
    """Keyword arguments passed to connfunc when it is called. Read-only."""
    return self._connkwargs

  @property
  def healthcheck_callback(self):
    return self._healthcheck_callback

  @property
  def poolinit(self):
    """Initial size of the pool used at pool initialization. Read-only."""
    with self._lock:
      return self._poolinit

  @property
  def poolmin(self):
    """Minimum size of the pool. Can be updated. Cannot be set less than 0, nor set greater than poolmax. Update poolmax first if necessary."""
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
    """Maximum size of the pool. Can be updated. Cannot be set less than 1, nor set less than poolmin. Free connections in excess
       of max will be closed on the next cleanup run."""
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
    """Wakeup interval for cleanup thread, in seconds. Can be updated. Must be greater than 0."""
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
    """Idle timeout in seconds, after which idle connections are eligible for cleanup. Can be updated. Cannot be less than 0."""
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

  def _cleanpool(self):
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
        for x in sorted(sorted(enumerate(self._free),key=lambda z: (now-z[1]._touch_time, z[1]._create_time), reverse=True)[0:poolmax_excess],key=lambda z: z[0],reverse=True):
          del self._pool[x[1]]
          del self._free[x[0]]

      # add connections till we get back up to _poolmin
      addtarget=self._poolmin - len(self._pool)
      if addtarget > 0:
        for x in range(0,addtarget):
          try:
            self._addconnection()
          except:
            logger.exception("Failed to add connection while pool size below poolmin")
    self._cleanup_cycles+=1

  def _finddbmodule(self):
    with self._lock:
      if len(self._free) == 0:
        with contextlib.closing(self._connfunc(*self._connargs,**self._connkwargs)) as conn:
          modulename=conn.__class__.__module__
      else:
        modulename=self._free[0]._conn.__class__.__module__
      components=modulename.split('.')
      while len(components) > 0:
        dbmodule=importlib.import_module('.'.join(components))
        if 'connect' in dir(dbmodule):
          return dbmodule
        del components[-1]
      raise Exception("Could not determine main dbmodule")

  def getconnection(self,blocking=True,timeout=None):
    """
    Return a connection from the pool.

    :param blocking: Whether to block if no connections are available. Defaults to True, meaning calls may block.
    :param timeout: Timeout after which call will return None if no connections are available. No effect if blocking is False. Must be 0 or greater.

    :returns: Returns a NimueConnection object, or None if one is not available in nonblocking mode, or when a timeout is reached.

    :raises Exception: If there are no available connections in the pool, and there is sufficient capacity, a new connection attempt is made. Opening the new connection could raise an exception. In this case, the calling code is responsible for catching the exception and retrying.
    """
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

      while True:
        # if there's a free connection in the pool, we are good
        if len(self._free) > 0:
          member=self._free.pop(0)
          if self._healthcheck_on_getconnection:
            # if we fail healthcheck, remove from pool and start over
            if not member.healthcheck():
              member.close()
              del self._pool[member]
              continue
          self._use[member]=1
          return NimueConnection(self,member)
        # if there's not, but there's room to add a new connection, we are also good
        elif len(self._pool.keys()) < self._poolmax:
          self._addconnection()
          member=self._free.pop(0)
          if self._healthcheck_on_getconnection:
          # go ahead and healthcheck the new connection as well
            if not member.healthcheck():
              member.close()
              del self._pool[member]
              continue
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
          # At this point, there might be a connection available, so do nothing, go
          # back to the top of the loop, and hopefully get a connection

  def poolstats(self):
    """
    Get runtime stats from connection pool.

    :returns: A NimueConnectionPoolStats object.
    """
    with self._lock:
      poolsize=len(self._pool)
      poolused=len(self._use)
      poolfree=len(self._free)
      return NimueConnectionPoolStats(poolsize,poolused,poolfree,self._connections_cleaned_dead,self._connections_cleaned_idle,self._cleanup_cycles)

  def close(self):
    """
    Close the connection pool, and shutdown the associated cleanup thread. Note that this method will block until all outstanding connections
    have been returned to the pool.
    """
    # shutdown the cleanup thread
    self._exitevent.set()
    self._cleanupthread.join()

class _NimueConnectionPoolMember:
  def __init__(self,owner,conn):
    self._owner=owner
    self._conn=conn
    self._create_time=time.monotonic()
    self._touch_time=self._create_time
    self._check_time=self._create_time
    self._healthcheck_callback=self._owner.healthcheck_callback

  def healthcheck(self):
    with self._owner._lock:
      dbmodule=self._owner._dbmodule
    r=self._healthcheck_callback(self._conn,dbmodule)
    self._check_time=time.monotonic()
    return r

  def touch(self):
    self._touch_time=time.monotonic()

  def close(self):
    self._conn.close()

class NimueConnection:
  """
  Wrapper around a DBAPI 2.0 compliant Connection object. Should not be initialized directly, but is
  returned by NimueConnectionPool.getconnection(). Should behave identically to the underlying Connection
  in most respects, but the close() method returns the Connection to the pool, rather than actually
  closing it. Do not attempt to call methods on objects of this class after close().

  It is important not to leak connections - they should be explicitly closed when no longer in use, to
  return them to the pool. The object destructor issues a warning if a connection object is destroyed
  without first being closed.
  """
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
      self._conn.__setattr__(attr,value)

  def __del__(self):
    if not self._closed:
      logger.warning("NimueConnection destructor reached without explicit close.")
      self.close()

  def __enter__(self):
    self._conn.__enter__()
    return self

  def __exit__(self,exc_type,exc_value,traceback):
    self._conn.__exit__(exc_type,exc_value,traceback)

  def close(self):
    """Return this connection to the pool. Do not attempt to call methods on the object after close()."""
    # if _closed is True, we do nothing and return
    if self._closed:
      return

    # if the pool has been closed, just close the
    # underlying connection, mark ourself closed and move on
    if self._pool._exitevent.is_set():
      self._conn.close()
      self._closed=True
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
  """
  Object containing stats about a NimueConnectionPool. Should not be initialized directly, but is returned
  by NimueConnectionPool.getstats().

  :attribute poolsize: Current total number of connections in the pool (both used and free).
  :attribute poolused: Current number of connections from the pool currently in use.
  :attribute poolfree: Current number of available (free) connections in the pool.
  :attribute connections_cleaned_dead: Number of dead connections that have been removed from the pool during
  cleanup cycles during the pool's lifetime.
  :attribute connections_cleaned_idle: Number of idle connections that have been removed from the pool during
  cleanup cycles during the pool's lifetime.
  :attribute cleanup_cycles: Number of cleanup cycles that have run during the pool's lifetime.
  """
  def __init__(self,poolsize,poolused,poolfree,connections_cleaned_dead,connections_cleaned_idle,cleanup_cycles):
    self.poolsize=poolsize
    self.poolused=poolused
    self.poolfree=poolfree
    self.connections_cleaned_dead=connections_cleaned_dead
    self.connections_cleaned_idle=connections_cleaned_idle
    self.cleanup_cycles=cleanup_cycles
