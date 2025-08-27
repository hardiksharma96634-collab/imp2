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
    return dbutils.widgets.get("Platform Family")
  
  def getStackType(self):
    return dbutils.widgets.get("Stack")
  
  def getEventType(self):
    return dbutils.widgets.get("Event")
  
  def getOrgType(self):
    return dbutils.widgets.get("Originator")
  
  def getStartDate(self):
    return dbutils.widgets.get("Start Date")
  
  def getEndDate(self):
    return dbutils.widgets.get("End Date")
  
  def getErrorMessage(self):
    return dbutils.widgets.get("Error Message")
