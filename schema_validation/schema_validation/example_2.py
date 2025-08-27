# Databricks notebook source
# MAGIC %md ## Issue Configurations
# MAGIC - **Originator**: imagingDevice
# MAGIC - **Event**: supply_fleet_service
# MAGIC - **Family**: ULLYSES SELENE
# MAGIC - **stack**: Prod
# MAGIC - **Start Date**: 2024-05-01 
# MAGIC - **End Date**: 2024-06-17
# MAGIC - **errorMessage**: lifeInfo.lifeCount.lifeUnit
# MAGIC - **Link**: DOSDP-64768

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


