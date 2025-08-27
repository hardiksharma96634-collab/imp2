from schemaValidLib.generic.common_util import Common_Utils
from IPython.display import display

test_util = Common_Utils()

print(test_util.getEnvPath("stage"))
print(test_util.getOrgPath("imagingDevice"))
print(test_util.getEventPath("supply"))

test_util.widget.displayWidgets()

cdmPath = test_util.getCDMpath(originator="imagingDevice",event="lifeTimeCounterSnapshot",env="prod")
print(cdmPath)

df_test = test_util.loadDf()

display(test_util.combine_ref(df_test.limit(1)))

print(test_util.find_column_paths(df_test, "serialNumber"))
print(test_util.find_column_paths(df_test, "notificationTrigger"))

display(test_util.get_flattend_df(df_test.select("originator")))

display(test_util.add_fwReleaseOnCol(df_test.select("eventMeta","originator"), sample_fw_formate= "6.23.1.17-202408100423").limit(5))

display(test_util.add_fwReleaseOnCol(df_test.select("eventMeta","originator"), sample_fw_formate= "YOSHINXXXN002.2404A.00").limit(5))

display(test_util.add_fwReleaseOnCol(df = df_test.select("eventMeta","originator"),sample_fw_formate=None).limit(5))

display(test_util.add_fwVerLineCol(df_test.select("eventMeta","originator"), sample_fw_formate= "6.23.1.17-202408100423").limit(5))

display(test_util.add_fwVerLineCol(df_test.select("eventMeta","originator"), sample_fw_formate= "YOSHINXXXN002.2404A.00").limit(5))

display(test_util.add_fwVerLineCol(df = df_test.select("eventMeta","originator")).limit(5))

display(test_util.segrate_productNumber(df_test).limit(5))

display(test_util.loadDf().limit(5))