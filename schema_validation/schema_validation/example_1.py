# Databricks notebook source
# MAGIC %md 
# MAGIC #Welcome to 1st demo:
# MAGIC
# MAGIC <strong>Step-1: </strong>Run below command this to import schemaValidation package to your Notebook:<br> 
# MAGIC <strong>from schemaValidLib.schemaValidate import </strong><br><br>
# MAGIC <strong>Step-2: </strong>Select widgets as per the follwing:<br>
# MAGIC * Event: infoSystemEvents
# MAGIC * Originator: imagingDevice
# MAGIC * Platform Family: Moreto
# MAGIC * Stack: stage
# MAGIC * Stard Date: 2023-12-20
# MAGIC * End Date: 2023-12-25
# MAGIC * Error Message: Missing field 'version'
# MAGIC
# MAGIC *Note*: This Markdown is given only in demo notebook. 
# MAGIC

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

schemaTemplate.helper.display_links()

# COMMAND ----------

df=schemaTemplate.loadDf_using_unity()

# COMMAND ----------

# MAGIC %md
# MAGIC team_onecloud_prod.cdm_bronze.domain_job_v1_domain_imagingdevice_v1_context_v1_privacy_v1

# COMMAND ----------

print(df.collect()[0][0])

# COMMAND ----------

print(f"""
                    select * from '{unity_catalog_table_path}' where recieveDate >= '{startDate}' and recieveDate <='{endDate}'
                    """)

# COMMAND ----------

 df = spark.sql(f"""
                    select distinct(catalog_component) from team_enterprise_prod.ref_enrich.platform_gun_mappings
                    where originator_gun like '{originator_path}' and gun_name like '{event_path}'
                    """)
unity_catalog_component = df.collect()[0][0]
unity_catalog_table_path = "team_onecloud_"+env+"."+unity_catalog_component

# COMMAND ----------

print(f"""
                    select * from '{unity_catalog_table_path}' where recieveDate >= '{startDate}' and recieveDate <='{endDate}'
                    """)
