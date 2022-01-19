"""
Exception classes for Nimue.
"""

class NimueInvalidParameterValue(Exception):
  """Indicates that a value passed for a parameter was not accepted."""
  pass

class NimueDBModuleFailure(Exception):
  """Indicates failure to identify the underlying dbmodule in use."""
  pass

class NimuePoolClosedError(Exception):
  """Indicates an operation was attempted on a pool that was already closed."""
  pass

class NimueNoConnectionAvailable(Exception):
  """Raised when getconnection() fails to return a connection, due to timeout or another reason."""
  pass
