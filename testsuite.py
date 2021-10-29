#!/usr/bin/env python3

# Copyright (c) 2021 James Lucas

import contextlib
import nimue
import nimue.nimue
import os.path
import shutil
import sqlite3
import tempfile
import time
import unittest
import unittest.mock

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
    curs=self.conn.cursor()
    curs.execute("create table updtest (id integer)")
    self.conn.commit()

  @unittest.mock.patch('nimue.nimue._NimueCleanupThread')
  def testGetters(self,FakeThread):
    "Test NimueConnectionPool property getters."
    with nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolmin=2,poolmax=4) as pool:
      self.assertEqual(pool.connfunc,sqlite3.connect)
      self.assertEqual(pool.connargs,(os.path.join(self.tempdir,'testdb'),))
      self.assertEqual(pool.connkwargs,{'check_same_thread': False})
      self.assertEqual(pool.poolmin,2)
      self.assertEqual(pool.poolmax,4)
      self.assertEqual(pool.cleanup_interval,60)
      self.assertEqual(pool.idle_timeout,300)

  @unittest.mock.patch('nimue.nimue._NimueCleanupThread')
  def testInitialSizeMin(self,FakeThread):
    "Test poolmin during pool initialization."
    with nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolmin=2,poolmax=4) as pool:
      self.assertEqual(pool.poolstats().poolsize,2)

  @unittest.mock.patch('nimue.nimue._NimueCleanupThread')
  def testInitialSizeInit(self,FakeThread):
    "Test poolinit during pool initialization."
    with nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolmin=2,poolmax=4,poolinit=3) as pool:
      self.assertEqual(pool.poolstats().poolsize,3)

  @unittest.mock.patch('nimue.nimue._NimueCleanupThread')
  def testMaxSize(self,FakeThread):
    "Test poolmax during pool initialization."
    x=[]
    with nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolmin=2,poolmax=4) as pool:
      with contextlib.ExitStack() as stack:
        for y in range(0,4):
          x.append(pool.getconnection())
          stack.push(contextlib.closing(x[-1]))
        self.assertEqual(pool.poolstats().poolsize,4)
    
  @unittest.mock.patch('nimue.nimue._NimueCleanupThread')
  def testIdleCleanup(self,FakeThread):
    "Test cleanup of idle connections."
    x=[]
    with nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolmin=2,poolmax=4,idle_timeout=0) as pool:
      with contextlib.ExitStack() as stack:
        for y in range(0,4):
          x.append(pool.getconnection())
          stack.push(contextlib.closing(x[-1]))
        self.assertEqual(pool.poolstats().poolsize,4)
      pool._healthcheckpool()
      self.assertEqual(pool.poolstats().poolsize,2)

  @unittest.mock.patch('nimue.nimue._NimueCleanupThread')
  def testOverMax(self,FakeThread):
    "Test cleanup of connections beyond poolmax."
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
      pool._healthcheckpool()
      self.assertEqual(pool.poolstats().poolsize,4)
      pool.idle_timeout=0
      pool._healthcheckpool()
      self.assertEqual(pool.poolstats().poolsize,2)
      self.assertEqual(pool.poolstats().poolfree,2)
      self.assertEqual(pool.poolstats().poolused,0)

  @unittest.mock.patch('nimue.nimue._NimueCleanupThread')
  def testDefaults(self,FakeThread):
    "Test NimueConnectionPool defaults."
    x=[]
    with nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False}) as pool:
      self.assertEqual(pool.poolinit,None)
      self.assertEqual(pool.poolmin,10)
      self.assertEqual(pool.poolmax,20)
      self.assertEqual(pool.cleanup_interval,60)
      self.assertEqual(pool.idle_timeout,300)

  @unittest.mock.patch('nimue.nimue._NimueCleanupThread')
  def testParamValidation(self,FakeThread):
    "Test validation of parameters at NimueConnectionPool construction."
    x=[]
    with self.assertRaises(Exception):
      nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolmin=-1,poolmax=10)
    with self.assertRaises(Exception):
      nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolmin=5,poolmax=4)
    with self.assertRaises(Exception):
      nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolmin=11,poolmax=10)
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

  def tearDown(self):
    self.conn.close()
    shutil.rmtree(self.tempdir)

class ConnectionTests(unittest.TestCase):
  @unittest.mock.patch('nimue.nimue._NimueCleanupThread')
  def setUp(self,FakeThread):
    self.tempdir=tempfile.mkdtemp()
    self.conn=sqlite3.connect(database=os.path.join(self.tempdir,'testdb'),check_same_thread=False)
    curs=self.conn.cursor()
    curs.execute("create table updtest (id integer)")
    self.conn.commit()
    self.pool=nimue.NimueConnectionPool(sqlite3.connect,(os.path.join(self.tempdir,'testdb'),),{'check_same_thread': False},poolmin=2,poolmax=10)

  def testGetConnection(self):
    "Test that getconnection returns a NimueConnection."
    with contextlib.closing(self.pool.getconnection()) as conn:
      self.assertTrue(isinstance(conn,nimue.NimueConnection))

  def testClose(self):
    "Test connection close returns connection to pool free list."
    self.assertEqual(self.pool.poolstats().poolfree,2)
    with contextlib.closing(self.pool.getconnection()) as conn:
      pass
    self.assertEqual(self.pool.poolstats().poolfree,2)

  def tearDown(self):
    self.conn.close()
    shutil.rmtree(self.tempdir)

if __name__=='__main__':
  unittest.main()
