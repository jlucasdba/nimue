import datetime
import sys
import threading

class DumbPoolHealthCheckThread(threading.Thread):
  def __init__(self,owner):
    super().__init__()
    self.exitevent=owner._exitevent
    self.owner=owner

  def run(self):
    while(not self.exitevent.wait(timeout=60)):
      self.owner._healthcheckpool()

class DumbPool(object):
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
    self._healthcheckthread=DumbPoolHealthCheckThread(self)
    self._healthcheckthread.start()

  def __del__(self):
    self.close()

  def __enter__(self):
    return self

  def __exit__(self,exc_type,exc_value,traceback):
    self.close()

  def _addconnection(self):
    member=PoolMember(dbmodule=self.dbmodule,conn=self.dbmodule.connect(*self.connargs,**self.connkwargs))
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
      for x in dead:
        self._addconnection()

  def getconnection(self):
    with self._lock:
      if len(self._free) > 0:
        member=self._free.pop(0)
        self._use[member]=1
        return PoolConnection(self,member)
      elif len(self._pool.keys()) < self.max:
        member=self._addconnection()
        member=self._free.pop(0)
        self._use[member]=1
        return PoolConnection(self,member)
      else:
        while(len(self._free) <= 0):
          self._lock.wait()
        member=self._free.pop(0)
        self._use[member]=1
        return PoolConnection(self,member)

  def close(self):
    for x in self._pool.keys():
      x.conn.close() 
    self._exitevent.set()
    self._healthcheckthread.join()

class PoolMember(object):
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
    except:
      r=False
    self.check_time=datetime.datetime.now()
    return r

  def touch(self):
    self.touch_time=datetime.datetime.now()

class PoolConnection(object):
  def __init__(self,pool,member):
    self._member=member
    self._pool=pool
    self.__dict__['_conn']=member.conn

  def __getattr__(self,attr):
    if attr in self.__dict__:
      return getattr(self, attr)
    return getattr(self._conn,attr)

  def __setattr__(self,attr,value):
    if attr in ('_pool','_conn','_member'):
      self.__dict__[attr]=value
    else:
      self._conn.__dict__[attr]=value

  def __enter__(self):
    return self

  def __exit__(self,exc_type,exc_value,traceback):
    self.close()

  def close(self):
    self._conn.rollback()
    with self._pool._lock:
      del self._pool._use[self._member]
      self._pool._free.insert(0,self._member)
      self._pool._lock.notify()
    # explicitly remove reference back to pool
    del self._pool
