# from pyspark.sql import SparkSession
from databricks.connect import DatabricksSession
from pyspark.dbutils import DBUtils 

class Spark:
    spark  = DatabricksSession.builder.getOrCreate()

dbutils = DBUtils(Spark)

