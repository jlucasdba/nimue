# Copyright (c) 2021 James Lucas

def healthcheck_callback_std(conn):
  with conn.cursor() as curs:
    curs.execute("SELECT 1")
  conn.rollback()

def healthcheck_callback_oracle(conn):
  with conn.cursor() as curs:
    curs.execute("SELECT 1 FROM DUAL")
  conn.rollback()
