class CustomError(Exception):
  """Base class for other exceptions"""
  pass

class originatorError(CustomError):
  """Raised when the Originator type is not found in .env file"""
  def __init__(self, org, message="originator not found"):
    self.value = org
    self.message = message
    super().__init__(self.message)

class eventError(CustomError):
  """Raised when the Event Gun is not found in .env file"""
  def __init__(self, event, message="Event not found"):
    self.value = event
    self.message = message
    super().__init__(self.message)

class stackError(CustomError):
  """Raised when the Stack environment type is not found in .env file"""
  def __init__(self, stack_env, message="Stack environment not found"):
    self.value = stack_env
    self.message = message
    super().__init__(self.message)
