# Databricks notebook source
# MAGIC %md ## Issue Configurations
# MAGIC - **Originator**: sirius
# MAGIC - **Event**: job
# MAGIC - **Family**: SAYAN
# MAGIC - **stack**: Prod
# MAGIC - **Start Date**: 2024-10-01 
# MAGIC - **End Date**: 2024-10-18
# MAGIC - **errorMessage**: DataValvesError

# COMMAND ----------

from schemaValidLib.schemaValidate import *

# COMMAND ----------

schemaTemplate.helper()

# COMMAND ----------

schemaTemplate.processValidation()

# COMMAND ----------

schemaTemplate.display_reports(display)

# COMMAND ----------

schemaTemplate.display_payloads(display)

# COMMAND ----------


