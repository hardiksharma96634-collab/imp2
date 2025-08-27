import markdown
from IPython.core.display import HTML
from schemaValidLib.generic.widget import Widgets
from schemaValidLib.utils import Utils
from schemaValidLib.generic.helper import Helper

class SchemaValidate(Utils):
  def __init__(self)->None:
    super().__init__()
    self.widget = Widgets()


  def processValidation(self, joinType=None,differentColumns=None,filterExp=None):
    try:
      load_df = self.loadDf()
      extract_col_df = self.get_extractData(load_df,differentColumns)
      self.getSchemaValidationProcessed(extract_col_df,differentColumns,filterExp,joinType)
      return
    except Exception as e:
      print(f"An error occurred: {str(e)}")

  def helper(self):
    self.helper=Helper()
            
schemaTemplate = SchemaValidate()
schemaTemplate.widget.displayWidgets()