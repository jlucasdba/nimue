#!/usr/bin/env python3

# Copyright (c) 2021 James Lucas

import contextlib
import os.path
import shutil
import sqlite3
import tempfile
import threading
import time
import unittest
import unittest.mock

import nimue
import nimue.callback

class FakeThread(nimue.nimue._NimueCleanupThread):
  def __init__(self,owner):
    pass

  def run(self):
    pass

  def start(self):
    pass

class PoolTests(unittest.TestCase):
  def setUp(self):
    self.tempdir=tempfile.mkdtemp()
    self.conn=sqlite3.connect(database=os.path.join(self.tempdir,'testdb'),check_same_thread=False)

  @unittest.mock.patch('nimue.nimue._NimueCleanupThread')
  def testGetters(self,FakeThread):
    """Test NimueConnectionPool property getters."""
    with nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolmin=2,poolmax=4) as pool:
      self.assertEqual(pool.connfunc,sqlite3.connect)
      self.assertEqual(pool.connargs,(os.path.join(self.tempdir,'testdb'),))
      self.assertEqual(pool.connkwargs,{'check_same_thread': False})
      self.assertEqual(pool.poolmin,2)
      self.assertEqual(pool.poolmax,4)
      self.assertEqual(pool.cleanup_interval,60)
      self.assertEqual(pool.idle_timeout,300)

  @unittest.mock.patch('nimue.nimue._NimueCleanupThread')
  def testSetterValidation(self,FakeThread):
    """Test validation of NimueConnectionPool property setters."""
    with nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolmin=0,poolmax=5) as pool:
      with self.assertRaises(Exception):
        pool.poolmin=-1
      with self.assertRaises(Exception):
        pool.poolmin=6
      with self.assertRaises(Exception):
        pool.poolmax=0

    with nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolmin=4,poolmax=5) as pool:
      with self.assertRaises(Exception):
        pool.poolmax=3

  @unittest.mock.patch('nimue.nimue._NimueCleanupThread')
  def testInitialSizeMin(self,FakeThread):
    """Test poolmin during pool initialization."""
    with nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolmin=2,poolmax=4) as pool:
      self.assertEqual(pool.poolstats().poolsize,2)

  @unittest.mock.patch('nimue.nimue._NimueCleanupThread')
  def testInitialSizeInit(self,FakeThread):
    """Test poolinit during pool initialization."""
    with nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolmin=2,poolmax=4,poolinit=3) as pool:
      self.assertEqual(pool.poolstats().poolsize,3)

  @unittest.mock.patch('nimue.nimue._NimueCleanupThread')
  def testMaxSize(self,FakeThread):
    """Test poolmax during pool initialization."""
    x=[]
    with nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolmin=2,poolmax=4) as pool:
      with contextlib.ExitStack() as stack:
        for y in range(0,4):
          x.append(pool.getconnection())
          stack.push(contextlib.closing(x[-1]))
        self.assertEqual(pool.poolstats().poolsize,4)
    
  @unittest.mock.patch('nimue.nimue._NimueCleanupThread')
  def testIdleCleanup(self,FakeThread):
    """Test cleanup of idle connections."""
    x=[]
    with nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolmin=2,poolmax=4,idle_timeout=0) as pool:
      with contextlib.ExitStack() as stack:
        for y in range(0,4):
          x.append(pool.getconnection())
          stack.push(contextlib.closing(x[-1]))
        self.assertEqual(pool.poolstats().poolsize,4)
      pool._cleanpool()
      self.assertEqual(pool.poolstats().poolsize,2)

  @unittest.mock.patch('nimue.nimue._NimueCleanupThread')
  def testOverMax(self,FakeThread):
    """Test cleanup of connections beyond poolmax."""
    x=[]
    with nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolmin=2,poolmax=10) as pool:
      with contextlib.ExitStack() as stack:
        for y in range(0,10):
          x.append(pool.getconnection())
          stack.push(contextlib.closing(x[-1]))
        self.assertEqual(pool.poolstats().poolsize,10)
      pool.poolmax=4
      self.assertEqual(pool.poolmax,4)
      self.assertEqual(pool.poolstats().poolsize,10)
      pool._cleanpool()
      self.assertEqual(pool.poolstats().poolsize,4)
      pool.idle_timeout=0
      pool._cleanpool()
      self.assertEqual(pool.poolstats().poolsize,2)
      self.assertEqual(pool.poolstats().poolfree,2)
      self.assertEqual(pool.poolstats().poolused,0)

  @unittest.mock.patch('nimue.nimue._NimueCleanupThread')
  def testOverMax(self,FakeThread):
    """Test cleanup of connections beyond poolmax when there are insufficient free connections."""
    x=[]
    with nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolmin=2,poolmax=10) as pool:
      with contextlib.ExitStack() as stack:
        for y in range(0,10):
          x.append(pool.getconnection())
          stack.push(contextlib.closing(x[-1]))
        self.assertEqual(pool.poolstats().poolsize,10)
        pool.poolmax=4
        self.assertEqual(pool.poolmax,4)
        self.assertEqual(pool.poolstats().poolsize,10)
        pool._cleanpool()
        self.assertEqual(pool.poolstats().poolsize,10)
        pool.idle_timeout=0
        pool._cleanpool()
        self.assertEqual(pool.poolstats().poolsize,10)
        self.assertEqual(pool.poolstats().poolfree,0)
        self.assertEqual(pool.poolstats().poolused,10)

  @unittest.mock.patch('nimue.nimue._NimueCleanupThread')
  def testDefaults(self,FakeThread):
    """Test NimueConnectionPool defaults."""
    x=[]
    with nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False}) as pool:
      self.assertEqual(pool.poolinit,None)
      self.assertEqual(pool.poolmin,10)
      self.assertEqual(pool.poolmax,20)
      self.assertEqual(pool.cleanup_interval,60)
      self.assertEqual(pool.idle_timeout,300)

  @unittest.mock.patch('nimue.nimue._NimueCleanupThread')
  def testParamValidation(self,FakeThread):
    """Test validation of parameters at NimueConnectionPool construction."""
    x=[]
    # poolmin cannot be less than 0
    with self.assertRaises(Exception):
      nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolmin=-1,poolmax=10)
    # poolmin cannot be greater than poolmax
    with self.assertRaises(Exception):
      nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolmin=11,poolmax=10)
    # poolmax cannot be less than 1
    with self.assertRaises(Exception):
      nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolmin=0,poolmax=0)
    # poolmax cannot be less than poolmin
    with self.assertRaises(Exception):
      nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolmin=5,poolmax=4)
    # poolinit cannot be less than poolmin
    with self.assertRaises(Exception):
      nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolinit=4,poolmin=5,poolmax=10)
    # poolinit cannot be greater than poolmax
    with self.assertRaises(Exception):
      nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolinit=11,poolmin=5,poolmax=10)
    with nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolmin=2,poolmax=10) as pool:
      with contextlib.ExitStack() as stack:
        for y in range(0,10):
          x.append(pool.getconnection())
          stack.push(contextlib.closing(x[-1]))

        def assertfunc():
          pool.poolmin=12
        self.assertRaises(Exception,assertfunc)

        def assertfunc():
          pool.poolmax=20
          pool.poolmin=12
        assertfunc()
        self.assertEqual(pool.poolmax,20)
        self.assertEqual(pool.poolmin,12)

        def assertfunc():
          pool.poolmax=2
        self.assertRaises(Exception,assertfunc)

        self.assertEqual(pool.poolmax,20)
        self.assertEqual(pool.poolmin,12)

        def assertfunc():
          pool.cleanup_interval=0
        self.assertRaises(Exception,assertfunc)

        def assertfunc():
          pool.idle_timeout=-1
        self.assertRaises(Exception,assertfunc)

  @unittest.mock.patch('nimue.nimue._NimueCleanupThread')
  def testGetConnection(self,FakeThread):
    """Test that getconnection returns a NimueConnection."""
    with nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolmin=1,poolmax=5,poolinit=1) as pool:
      with contextlib.closing(pool.getconnection()) as conn:
        self.assertTrue(isinstance(conn,nimue.NimueConnection))

  @unittest.mock.patch('nimue.nimue._NimueCleanupThread')
  def testGetAtFreeZero(self,FakeThread):
    """Test getconnection when no free connections"""
    with nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolmin=1,poolmax=5,poolinit=1) as pool:
      self.assertEqual(pool.poolstats().poolsize,1)
      self.assertEqual(pool.poolstats().poolused,0)
      self.assertEqual(pool.poolstats().poolfree,1)
      with contextlib.closing(pool.getconnection()) as conn:
        self.assertTrue(isinstance(conn,nimue.NimueConnection))
        self.assertEqual(pool.poolstats().poolsize,1)
        self.assertEqual(pool.poolstats().poolused,1)
        self.assertEqual(pool.poolstats().poolfree,0)
        with contextlib.closing(pool.getconnection()) as conn2:
          self.assertEqual(pool.poolstats().poolsize,2)
          self.assertEqual(pool.poolstats().poolused,2)
          self.assertEqual(pool.poolstats().poolfree,0)

  @unittest.mock.patch('nimue.nimue._NimueCleanupThread')
  def testGetAtMaxZeroTimeout(self,FakeThread):
    """Test getconnection when pool is at max size, with zero timeout"""
    x=[]
    with nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolmin=1,poolmax=5,poolinit=5) as pool:
      self.assertEqual(pool.poolstats().poolsize,5)
      self.assertEqual(pool.poolstats().poolused,0)
      self.assertEqual(pool.poolstats().poolfree,5)
      with contextlib.ExitStack() as stack:
        for i in range(0,5):
          self.assertEqual(pool.poolstats().poolsize,5)
          self.assertEqual(pool.poolstats().poolused,0+i)
          self.assertEqual(pool.poolstats().poolfree,5-i)
          x.append(pool.getconnection())
          stack.push(contextlib.closing(x[-1]))
        self.assertEqual(pool.poolstats().poolsize,5)
        self.assertEqual(pool.poolstats().poolused,5)
        self.assertEqual(pool.poolstats().poolfree,0)
        x.append(pool.getconnection(timeout=0))
        if x[-1] is not None:
          stack.push(contextlib.closing(x[-1]))
        self.assertTrue(x[-1] is None)
        self.assertEqual(pool.poolstats().poolsize,5)
        self.assertEqual(pool.poolstats().poolused,5)
        self.assertEqual(pool.poolstats().poolfree,0)

  @unittest.mock.patch('nimue.nimue._NimueCleanupThread')
  def testGetAtMaxNonZeroTimeout(self,FakeThread):
    """Test getconnection when pool is at max size, with (small) non-zero timeout"""
    x=[]
    with nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolmin=1,poolmax=5,poolinit=5) as pool:
      self.assertEqual(pool.poolstats().poolsize,5)
      self.assertEqual(pool.poolstats().poolused,0)
      self.assertEqual(pool.poolstats().poolfree,5)
      with contextlib.ExitStack() as stack:
        for i in range(0,5):
          self.assertEqual(pool.poolstats().poolsize,5)
          self.assertEqual(pool.poolstats().poolused,0+i)
          self.assertEqual(pool.poolstats().poolfree,5-i)
          x.append(pool.getconnection())
          stack.push(contextlib.closing(x[-1]))
        self.assertEqual(pool.poolstats().poolsize,5)
        self.assertEqual(pool.poolstats().poolused,5)
        self.assertEqual(pool.poolstats().poolfree,0)
        x.append(pool.getconnection(timeout=.25))
        if x[-1] is not None:
          stack.push(contextlib.closing(x[-1]))
        self.assertTrue(x[-1] is None)
        self.assertEqual(pool.poolstats().poolsize,5)
        self.assertEqual(pool.poolstats().poolused,5)
        self.assertEqual(pool.poolstats().poolfree,0)

  @unittest.mock.patch('nimue.nimue._NimueCleanupThread')
  def testGetConnectionThreaded(self,FakeThread):
    """Test getconnection with multiple threads."""
    def testthread(getevent,endevent,waitevent,pool):
      #set event immediately before entering wait
      waitevent.set()
      with contextlib.closing(pool.getconnection()) as conn:
        #set another event after getting connection
        getevent.set()
        endevent.wait()

    t=[]
    with nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolmin=1,poolmax=5,poolinit=1) as pool:
      # launch 5 threads which will consume all connections in pool
      for i in range(0,5):
        t.append({'getevent': threading.Event(),'waitevent': threading.Event(),'endevent': threading.Event()})
        t[-1]['thread']=threading.Thread(target=testthread,args=(t[-1]['getevent'],t[-1]['endevent'],t[-1]['waitevent'],pool))
        t[-1]['thread'].start()
      for i in range(0,5):
        t[i]['getevent'].wait()
      self.assertEqual(pool.poolstats().poolsize,5)
      self.assertEqual(pool.poolstats().poolused,5)
      self.assertEqual(pool.poolstats().poolfree,0)
      # launch a sixth connection, which will be blocked waiting for a connection
      t.append({'getevent': threading.Event(),'endevent': threading.Event(),'waitevent': threading.Event()})
      t[5]['thread']=threading.Thread(target=testthread,args=(t[5]['getevent'],t[5]['endevent'],t[5]['waitevent'],pool))
      t[5]['thread'].start()
      # possible non-determinism here - we can't know absolutely for
      # sure that the thread is blocked waiting.  But if waitevent is
      # set, that happens right before it calls getconnection, and if
      # getevent is not set, that means the connection hasn't been
      # obtained. So *probably* the thread is waiting.
      # We can at least verify that the poolstats haven't changed.
      t[5]['waitevent'].wait()
      self.assertEqual(pool.poolstats().poolsize,5)
      self.assertEqual(pool.poolstats().poolused,5)
      self.assertEqual(pool.poolstats().poolfree,0)
      self.assertFalse(t[5]['getevent'].isSet())

      # Finish out the first thread
      t[0]['endevent'].set()
      t[0]['thread'].join()
      # Now wait for the final thread to obtain its connection
      self.assertTrue(t[5]['getevent'].wait())
      self.assertEqual(pool.poolstats().poolsize,5)
      self.assertEqual(pool.poolstats().poolused,5)
      self.assertEqual(pool.poolstats().poolfree,0)

      # Now finish out remaining threads
      for i in range(1,6):
        t[i]['endevent'].set()
        t[i]['thread'].join()

      # Make sure the pool looks like we expect
      self.assertEqual(pool.poolstats().poolsize,5)
      self.assertEqual(pool.poolstats().poolused,0)
      self.assertEqual(pool.poolstats().poolfree,5)

  def tearDown(self):
    self.conn.close()
    shutil.rmtree(self.tempdir)

class ConnectionTests(unittest.TestCase):
  @unittest.mock.patch('nimue.nimue._NimueCleanupThread')
  def setUp(self,FakeThread):
    self.tempdir=tempfile.mkdtemp()
    self.conn=sqlite3.connect(database=os.path.join(self.tempdir,'testdb'),check_same_thread=False)
    self.pool=nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolmin=2,poolmax=10)

  def testClose(self):
    """Test connection close returns connection to pool free list."""
    self.assertEqual(self.pool.poolstats().poolfree,2)
    with contextlib.closing(self.pool.getconnection()) as conn:
      pass
    self.assertEqual(self.pool.poolstats().poolfree,2)

  @unittest.mock.patch('nimue.nimue._NimueCleanupThread')
  def testCloseWithClosedPool(self,FakeThread):
    pool=nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb2'),),{'check_same_thread': False},poolmin=1,poolmax=5,poolinit=5)
    conn=pool.getconnection()
    curs=conn.cursor()
    curs.execute("SELECT 1")
    pool.close()
    for x in curs:
      self.assertEqual(x[0],1)
    curs.close()
    conn.close()
    pool.close()

  def tearDown(self):
    self.conn.close()
    shutil.rmtree(self.tempdir)

class CallbackTests(unittest.TestCase):
  def setUp(self):
    self.tempdir=tempfile.mkdtemp()
    self.conn=sqlite3.connect(database=os.path.join(self.tempdir,'testdb'),check_same_thread=False)

  @unittest.mock.patch('nimue.nimue._NimueCleanupThread')
  def testStdHealthcheck(self,FakeThread):
    with nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolmin=1,poolmax=5,poolinit=5) as pool:
      with contextlib.closing(pool.getconnection()) as conn:
        r=conn._member.healthcheck()
        self.assertTrue(r)

  @unittest.mock.patch('nimue.nimue._NimueCleanupThread')
  def testOracleHealthcheck(self,FakeThread):
    with nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolmin=1,poolmax=5,poolinit=5,healthcheck_on_getconnection=False,healthcheck_callback=nimue.callback.healthcheck_callback_oracle) as pool:
      with contextlib.closing(pool.getconnection()) as conn:
        r=conn._member.healthcheck()
        self.assertFalse(r)
      with contextlib.closing(self.conn.cursor()) as curs:
        curs.execute("create table dual (id integer)")
        curs.execute("insert into dual values (1)")
      self.conn.commit()
      with contextlib.closing(pool.getconnection()) as conn:
        r=conn._member.healthcheck()
        self.assertTrue(r)

  def tearDown(self):
    self.conn.close()
    shutil.rmtree(self.tempdir)

if __name__=='__main__':
  unittest.main()
