import markdown
from IPython.core.display import display, HTML
from schemaValidLib.generic.widget import Widgets
from schemaValidLib.utils import Utils

class SchemaValidate:
  def __init__(self)->None:
    self.widget = Widgets()
    self.utils = Utils()
    self.display_readme()

  def processValidation(self, joinType=None,differentColumns=None,filterExp=None):
    try:
      load_df = self.utils.loadDf()
      extract_col_df = self.utils.get_extractData(load_df,differentColumns)
      self.utils.getSchemaValidationProcessed(extract_col_df,differentColumns,filterExp,joinType)
      return
    except Exception as e:
      print(f"An error occurred: {str(e)}")

  def display_readme(self):
    try:
      with open('README.md', 'r', encoding='utf-8') as readme_file:
        readme_content = readme_file.read()
        # Convert Markdown to HTML
        html_content = markdown.markdown(readme_content)
        # Display HTML in Jupyter/Databricks
        display(HTML(html_content))
    except FileNotFoundError:
        print("README.md file not found.")
        
schemaTemplate = SchemaValidate()
schemaTemplate.widget.displayWidgets()
