import datetime
from schemaValidLib.generic.constant import Constant
from schemaValidLib.generic.standard_spark import dbutils

class Widgets():
  def __init__(self):
    self.constant = Constant()

  def displayWidgets(self):
    self.setOrgWidget()
    self.setEventWidget()
    self.setStacksWidget()
    self.setProductFamiliesWidget()
    self.startDate()
    self.endDate()
    self.errorMessage()
  
  """Stack Env Widgets"""
  def setStacksWidget(self):
    stacks = list(self.constant.stack_env.keys())
    stacks.sort()
    defaultStack = stacks[0]
    return dbutils.widgets.dropdown("Stack", defaultStack, stacks)

  """Event Widgets"""
  def setEventWidget(self):
    events = list(self.constant.event.keys())
    events.sort()
    defaultEvent = events[0]
    return dbutils.widgets.dropdown("Event", defaultEvent, events)

  """Originator Widgets"""
  def setOrgWidget(self):
    try:
      originators = list(self.constant.originator.keys())
      originators.sort()
      defaultOrg = originators[0]
      return dbutils.widgets.dropdown("Originator", defaultOrg, originators)
    except IndexError as e:
      print(f"IndexError: {e}\n {self.constant.base_name}")

  def startDate(self):
    formatted_date = datetime.datetime.today().strftime('%Y-%m-%d')
    return dbutils.widgets.text("Start Date", formatted_date, "Start Date (YYYY-MM-DD)")

  def endDate(self):
    formatted_date = datetime.datetime.today().strftime('%Y-%m-%d')
    return dbutils.widgets.text("End Date", formatted_date, "end Date (YYYY-MM-DD)")

  def setProductFamiliesWidget(self):
    self.constant.platformFamilies.sort()
    return dbutils.widgets.multiselect("Platform Family", self.constant.platformFamilies[0], self.constant.platformFamilies)
  
  def errorMessage(self):
    return dbutils.widgets.text("Error Message","","errorMessage")
  
  def getPlatformFamily(self):
    try:
      return dbutils.widgets.get("Platform Family")
    except:
      print("Warning: Could not get Platform Family widget")
      return ""

  def getStackType(self):
    try:
      return dbutils.widgets.get("Stack")
    except:
      print("Warning: Could not get Stack widget")
      return ""

  def getEventType(self):
    try:
      return dbutils.widgets.get("Event")
    except:
      print("Warning: Could not get Event widget")
      return ""

  def getOrgType(self):
    try:
      return dbutils.widgets.get("Originator")
    except:
      print("Warning: Could not get Originator widget")
      return ""

  def getStartDate(self):
    try:
      return dbutils.widgets.get("Start Date")
    except:
      print("Warning: Could not get Start Date widget")
      return ""

  def getEndDate(self):
    try:
      return dbutils.widgets.get("End Date")
    except:
      print("Warning: Could not get End Date widget")
      return ""

  def getErrorMessage(self):
    try:
      return dbutils.widgets.get("Error Message")
    except:
      print("Warning: Could not get Error Message widget")
      return ""
