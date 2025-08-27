import schemaValidLib.generic.literals as literals

class Constant:
  def __init__(self):
    self.base_name = literals.BASE_PATH
    self.stack_env = literals.STACK
    self.originator = literals.ORIGINATOR
    self.event = literals.EVENT
    self.platformFamilies = literals.PLATFORM_FAMILY