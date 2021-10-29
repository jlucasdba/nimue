# Copyright (c) 2021 James Lucas

def healthcheck_callback_std(conn,dbmodule,logger):
  return _healthcheck_callback_base(conn,dbmodule,logger,"SELECT 1")

def healthcheck_callback_oracle(conn):
  return _healthcheck_callback_base(conn,dbmodule,logger,"SELECT 1 FROM DUAL")

def _healthcheck_callback_base(conn,dbmodule,logger,query):
  try:
    with conn.cursor() as curs:
      curs.execute(query)
    conn.rollback()
  except dbmodule.OperationalError:
    return False
  except:
    logger.exception('Unexpected exception during healthcheck. Connection will be invalidated.')
  return True
