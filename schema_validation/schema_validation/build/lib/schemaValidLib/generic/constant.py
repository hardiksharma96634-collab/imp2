import os
import json
from dotenv import load_dotenv

class Constant:
  def __init__(self):
    load_dotenv('/dbfs/FileStore/env/.env')
    self.base_name = os.getenv('BASE_PATH')
    self.stack_env = json.loads(os.getenv('STACK', '{}'))
    self.originator = json.loads(os.getenv('ORIGINATOR', '{}'))
    self.event = json.loads(os.getenv('EVENT', '{}'))
    self.platformFamilies = os.getenv('PLATFORM_FAMILY', '').split(",")
