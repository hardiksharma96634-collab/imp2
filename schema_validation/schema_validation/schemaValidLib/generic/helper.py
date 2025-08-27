import markdown
from IPython.core.display import HTML
import schemaValidLib.generic.links as links

class Helper:
  def __init__(self):
    self.help_all()

  def display_readme(self):
    try:
      with open('README.md', 'r', encoding='utf-8') as readme_file:
        readme_content = readme_file.read()
        html_content = markdown.markdown(readme_content)
        display(HTML(html_content))
    except FileNotFoundError:
        print("README.md file not found.")

  def display_links(self):
    print("Important Links\n")
    important_links = links.important_links
    for label, link in important_links.items():
      print(label,":",link,"\n")

  def help_all(self):
    for key,description in functions_description.items():
      print(key,":",description)

functions_description={
  "display_readme()" : "Displays the README.md file \n\tExample: schemaTemplate.helper.display_readme()\n",
  "display_links()" : "Displays the important links \n\tExample: schemaTemplate.helper.display_links()\n",
  "processValidation()" : "This function checks the particular the schema vaidation error provided in the widgets and generates schema validation report.\n",
  "loadDf()" : "This function reads the data from s3 bucket into dataframedata frame based on the originator, event category, stack environment, start date and end date selected from the widgets. It returns the dataframe if correct inputs are selected in widgets otherwise it raises the respective exceptions. \n",
  "display_payloads()" : "This function displays the schema validation report for each product family.\n\t Example: schemaTemplate.display_reports(display)\n",
  "display_reports(display_function)" : "This Function displays the payloads for each product family.\n\t Example: schemaTemplate.display_payloads(display)\n"
}
