from schema_validation.schemaValidLib.schemaValidate import SchemaValidate
from IPython.display import display

test_schema_main = SchemaValidate()

test_schema_main.widget.displayWidgets()

display(test_schema_main.utils.loadDf())

display(test_schema_main.schemaValidate(schemaError="Missing field "))
