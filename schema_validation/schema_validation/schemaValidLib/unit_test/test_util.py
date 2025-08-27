from schemaValidLib.utils import Utils
from IPython.display import display
from pyspark.sql.functions import col

test_util = Utils()

test_util.widget.displayWidgets()

print(test_util.widget.constant.base_name)

print(test_util.widget.getStackType())

print(test_util.get_requiredProductFamilyNames())

test_util.print_reproducibilty_schemaValidationIssue(50,150,'MALBEC')

df_test = test_util.loadDf()

print(test_util.common_columns)
print(test_util.get_sampleFw(df_test))
extracted_df = test_util.get_extractData(df_test)
display(extracted_df.limit(1))

test_util.getSchemaValidationReport(extracted_df,schemaError='KeyError: Missing field')

display(extracted_df.filter(col("eventDetailError").isNotNull()).groupBy("eventDetailError").count())

errorDf = extracted_df.filter(col("eventDetailError").rlike("KeyError: Missing field"))
validDf = extracted_df.filter(col("eventDetailError").isNotNull())

payload = test_util.getSameDeviceExpectedPayload(errorDf,validDf)
print(payload)

test_util.display_payload(payload,"KeyError: Missing field 'eventDetail' required for schema validation")

test_util.get_columnReqInSchemaValidnReport(df_test)
