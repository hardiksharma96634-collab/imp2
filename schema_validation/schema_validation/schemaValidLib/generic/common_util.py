
from schemaValidLib.generic.exceptions import originatorError, eventError, stackError
from schemaValidLib.generic.constant import Constant
from schemaValidLib.generic.widget import Widgets
from schemaValidLib.generic.standard_spark import Spark,dbutils


import fnmatch
from pyspark.sql.functions import col, first, concat, lit
from pyspark.sql.types import StructType
import etl_utils



class Common_Utils(Spark):

  def __init__(self)->None:
    self.constant = Constant()
    self.widget = Widgets()
    self.is_allPlatormFamilies = False
    self.df_path = self.constant.base_name+"/events/"
    self.products = []

  def combine_ref(self,initial_df):
    try:
      cdf_pmr_ref = Spark.spark.sql("select * from ref_enrich.cdf_pmr").select(col("product_number"), col("platform_subset_reporting_name").alias("platform_family"))
      rdma_prod_ref = Spark.spark.sql("select * from ref_enrich.rdma_product_ref").select(col("base_prod_nr").alias("product_number"), col("pltfrm_subset_nm").alias("platform_family"))
      tri_printer_ref = Spark.spark.sql("select * from ref_enrich.tri_printer_ref").select(col("printer_product_number").alias("product_number"), col("printer_platform_name").alias("platform_family"))
      combined_ref = cdf_pmr_ref.union(rdma_prod_ref).union(tri_printer_ref).groupBy("product_number").agg(first(col("platform_family")).alias("platform_family"))
      
      if "originator.originatorDetail.productNumber" in self.find_column_paths(initial_df,"productNumber"):
        return initial_df.join(combined_ref, col("originator.originatorDetail.productNumber").eqNullSafe(col("product_number")))
      else:
        return initial_df.join(combined_ref, col("originator.originatorDetail.modelNumber").eqNullSafe(col("product_number")))
    except Exception as e:
      print(f"Exception Occurs in combine_ref: {e}")

  def find_column_paths(self, df, col_name):
      # Recursively search for the column in the schema
      def search_schema(schema, path_so_far):
          paths = []
          for field in schema.fields:
              if field.name == col_name:
                  paths.append(path_so_far + [col_name])
              elif isinstance(field.dataType, StructType):
                  sub_paths = search_schema(field.dataType, path_so_far + [field.name])
                  if sub_paths:
                      paths.extend(sub_paths)
          return paths if paths else []
      # Call the search function with the schema of the DataFrame
      return ['.'.join(path) for path in search_schema(df.schema, [])]

  def group_by_product_number_and_firmware_version(self, df, column_name ,nullCompare:bool=True):
      # Select the relevant columns from the reference table
      df_rdma = Spark.spark.table("ref_enrich.rdma_product_ref").select(col("base_prod_nr"),col("prod_pltfrm_nm"))
      # Group the DataFrame by product number and firmware version, then count the rows in each group
      if(nullCompare):
        compare_df = df.filter(col(column_name).isNull())
      else:
        compare_df = df.filter(col(column_name).isNotNull())
    #  error_counts = (df.filter(col(column_name).isNull())
      error_counts = (compare_df
                        .select(col("originator.originatorDetail.productNumber"),
                                    col("originator.originatorDetail.firmwareVersion"))
                        .groupBy(col("productNumber"),col("firmwareVersion")).count())
                      
      # Rename the "productNumber" column to "error_productNumber", and "count" to "error_count"
      error_counts = (error_counts
                        .withColumnRenamed("productNumber", "error_productNumber")
                        .withColumnRenamed("count", "error_count"))
      # Count the total number of rows for each product number
      total_counts = (df.groupBy(col("originator.originatorDetail.productNumber")).count())
      # Join the two count DataFrames together on "error_productNumber" and "productNumber"
      joined_counts = (error_counts.join(total_counts,
                        error_counts["error_productNumber"] == total_counts["productNumber"],
                        "left"))
      # Calculate the error percentage and drop the unnecessary columns
      joined_counts = (joined_counts
                      .withColumn("error_percentage", (col("error_count") / col("count")) * 100)
                      .drop("error_productNumber", "error_count", "count"))
      # Join with the reference table and drop the "base_prod_nr" column
      joined_counts = (joined_counts.join(df_rdma,
              joined_counts["productNumber"] == df_rdma["base_prod_nr"],
              "left").drop("base_prod_nr"))
      # Sort by error percentage in descending order and return the DataFrame
      return joined_counts.sort(col("error_percentage").desc())

  def get_all_cdm_paths(self):
      dbutils_paths = dbutils.fs.ls(self.constant.base_name)
      all_paths = []
      originators = [org for org in self.constant.originator.keys()]
      events = [event_type for event_type in self.constant.event.keys()]
      
      for path in dbutils_paths:
          if path.isDir():
            for sub_path in dbutils.fs.ls(path.path):
              # print(f"{sub_path.path}\n")
              for i in originators:
                for j in events:
                  if (i in sub_path.path) & (j in sub_path.path):
                    all_paths.extend([sub_path.path])
      return all_paths

  def get_allColumn(self, df):
      # Recursively search for the column in the schema
      def search_schema(schema, path_so_far):
        paths = []
        for field in schema.fields:
          if isinstance(field.dataType, StructType):
            sub_paths = search_schema(field.dataType, path_so_far + [field.name])
            if sub_paths:
              paths.extend(sub_paths)
          else:
            paths.append(path_so_far + [field.name])
        return paths if paths else None
      # Call the search function with the schema of the DataFrame
      return ['.'.join(path) for path in search_schema(df.schema, [])]

  def get_flattend_df(self, df):
    df_cols = self.get_allColumn(df)
    flattened_df =  df.select([col(col_name).alias(col_name.replace('.', '_')) for col_name in df_cols])
    return flattened_df

  def add_fwReleaseOnCol(self,df,sample_fw_formate=None):
    fwVerPath = self.find_column_paths(df,"firmwareVersion")[0]
    if (sample_fw_formate is not None) and (fnmatch.fnmatch(sample_fw_formate,"*.*.00")):
        df_modified = df.withColumn("fwReleaseOn",col(fwVerPath).substr(-8,4))
    else:
      df_modified = df.withColumn("fwReleaseOn",col(fwVerPath).substr(-12, 8))
    return df_modified

  def add_fwVerLineCol(self,df,sample_fw_formate = None):
    fwVerPath = self.find_column_paths(df,"firmwareVersion")[0]
    if (sample_fw_formate is not None) and (fnmatch.fnmatch(sample_fw_formate,"*.*.00")):
      df_modified = df.withColumn("fwVerLine",col(fwVerPath).substr(-12,8))
    else:
      df_modified = df.withColumn("fwVerLine",col(fwVerPath).substr(0, 5))
    return df_modified

  def add_null_counts(self,df, column):
    df_with_null_counts = df.withColumn(column+'_null_count', sum(col(c).isNull().cast('integer') for c in self.get_allColumn(df.select(column))))
    df_with_total_null_count = df_with_null_counts.withColumn(column+'_null_counts', sum(col(column+'_null_count')))
    return df_with_total_null_count

  """
  this function is used to check whether the df have a field: "modelNumber" in originator or not. """
  def is_modelNumber_field(self,df):
    isFound = False
    if "originator.originatorDetail.modelNumber" in self.get_allColumn(df):
      isFound =True
    return isFound

  def segrate_productNumber(self,originalDf):
    if self.is_modelNumber_field(originalDf):
      newDf = originalDf.withColumn("productNumber",originalDf["originator.originatorDetail.modelNumber"])
    else:
      newDf = originalDf.withColumn("productNumber",originalDf["originator.originatorDetail.productNumber"])
    return newDf

  def get_sampleFw(self,df):
    fwVer =df.filter(col("originator.originatorDetail.firmwareVersion").isNotNull()).select("originator.originatorDetail.firmwareVersion").first()[0]
    return fwVer

  def getCDMpath(self,originator,event,env):
    try:
      base_path = self.constant.base_name
      org_path = self.getOrgPath(originator)
      event_path = self.getEventPath(event)
      stack_path = self.getEnvPath(env)
      print(f'originator gun: {org_path}')
      print(f'event gun: {event_path}')
    except originatorError as e:
      print(f"Error: {e.message} ")#given value: {e.value} Please provide originator any of{list(self.constant.originator.keys())}
    except stackError as e:
      print(f"Error: {e.message}")# given value: {e.value} Please provide env as {list(self.constant.stack_env.keys())}
    except eventError as e:
      print(f"Error: {e.message}") # given value: {e.value} Please provide event any of {list(self.constant.event.keys())}
    return (f"{base_path}/events/{org_path}/{event_path}/{stack_path}/")

  """
  this function returns stack environment path based on parameter passed to it i.e. stack name
  """
  def getEnvPath(self,env):
    if env in self.constant.stack_env:
      env = self.constant.stack_env[env]
      return env
    else:
      raise stackError(env)
    
  """
  this function returns event path based on parameter passed to it i.e. event name
  """
  def getEventPath(self, eventName):
    eventGun = self.constant.event[eventName]
    if (eventName.lower()!="other"):
      if (eventGun in self.df_path):
        return eventGun
      else:
        eventGun = self.constant.event[eventName]
        if eventGun not in self.df_path:
          self.df_path+=eventGun+'/'
          return eventGun
    else:
      eventGun = input("Enter the Event Gun:")
      if (eventGun in self.df_path):
          return eventGun
      else:
        file_list = dbutils.fs.ls(self.df_path)
        for file_info in file_list:
          if eventGun == file_info.name[:-1]:
            if eventGun not in self.df_path:
              self.df_path+=eventGun+'/'
            return eventGun
      print(f"Invalid Event Path...\n{self.df_path}\nNo event Path found for:{eventGun}\nPlease try Again..\n")
      return self.getEventPath(eventName)
  
  """
  this functions returns the originator path using originator name that will be given as parameter
  """
  def getOrgPath(self, orgName):
    orgGun = self.constant.originator[orgName]
    if (orgName.lower()!="other"):
      if (orgGun in self.df_path):
        return orgGun
      else:
        orgGun = self.constant.originator[orgName]
        if orgGun not in self.df_path:
          self.df_path+=orgGun+'/'
          return orgGun
    else:
      orgGun = input("Enter the originator Gun:")
      if (orgGun in self.df_path):
          return orgGun
      else:
        file_list = dbutils.fs.ls(self.df_path)
        for file_info in file_list:
          if orgGun == file_info.name[:-1]:
            if orgGun not in self.df_path:
              self.df_path+=orgGun+'/'
            return orgGun
      print(f"Invalid Originator Path...\n{self.df_path}\nNo originator Path found for:{orgGun}\nPlease try Again..\n")
      return self.getOrgPath(orgName)
    
  """filtered_forPlatformFamilies(dataFrame): Will return dataframe with platform families choosen from widget and if no plaform family is selected then it will return events for all platform families."""
  def filtered_forPlatformFamilies(self, dataFrame):
    platform_families = self.widget.getPlatformFamily()

    if isinstance(platform_families, str):
      if "other" in platform_families.lower():
        new_products = input("Enter platform families (should be comma(,) seprated in case of two or more values): ")
        new_products = new_products.upper().replace(" ", "")
        platform_families +=  new_products
        platform_families = platform_families.replace("Other", "")
      platform_families = platform_families.split(",")
      self.products = platform_families
      
    if platform_families == ['']:
      platform_families = self.constant.platformFamilies
      self.is_allPlatormFamilies = True
    print(f"filtered for platform families: {self.products}")
    df = self.combine_ref(dataFrame).filter(col("platform_family").rlike("|".join(platform_families)))
    return df

  """loadDf(): Will return completed DF specifically for the attributes choose from widgets"""
  def loadDf(self):
    originator = self.widget.getOrgType()
    event = self.widget.getEventType()
    env = self.widget.getStackType()
    try:
      df_path = self.getCDMpath(originator,event,env)
      startDate = self.widget.getStartDate()
      endDate = self.widget.getEndDate()
      print(f'Investigated time range: {startDate} to {endDate}')
      df = etl_utils.sage.Queue(df_path).load(start_date=startDate, end_date=endDate) 
      df = self.filtered_forPlatformFamilies(df).withColumn("recieveDate", concat(col("year"), lit("-"), col("month"), lit("-"), col("day")).cast("date"))
      return df
    except originatorError as e:
      print(f"Error: {e.message} given value: {e.value} Given Originator does not exits in this template.(constants.py)")
    except eventError as e:
      print(f"Error: {e.message} given value: {e.value} Given Event does not exits in this template.(constants.py)")
    except stackError as e:
      print(f"Error: {e.message} given value: {e.value} Given stack does not exits in this template.(constants.py)")
    except Exception as e:
      print(f"An error occurred: {str(e)}")

  
  def loadDf_using_unity(self):
    try:
      originator_path = self.getOrgPath(self.widget.getOrgType())
      event_path = self.getEventPath(self.widget.getEventType())
      env = self.widget.getStackType()
      startDate = self.widget.getStartDate()
      endDate = self.widget.getEndDate()
      platform_families = self.widget.getPlatformFamily()
      print(platform_families)
      print(f'Investigated time range: {startDate} to {endDate}')
      # df = etl_utils.sage.Queue(df_path).load(start_date=startDate, end_date=endDate)
      df = Spark.spark.sql(f"""
                    select distinct(catalog_component) from team_enterprise_prod.ref_enrich.platform_gun_mappings
                    where originator_gun like '{originator_path}' and gun_name like '{event_path}'
                    """)
      unity_catalog_component = df.collect()[0][0]
      unity_catalog_table_path = "team_onecloud"+env+"."+"cdm_bronze."+unity_catalog_component
      # unity_catalog_df = Spark.spark.sql(f"select * from '{unity_catalog_table_path}' where receive_date >= '{startDate}' and receive_date <='{endDate}'")

      unity_catalog_df = Spark.spark.sql(f"select * from {unity_catalog_table_path}
                                         where receive_date >= '{startDate}' and receive_date <='{endDate}'")
      # df = self.filtered_forPlatformFamilies(df).withColumn("recieveDate", concat(col("year"), lit("-"), col("month"), lit("-"), col("day")).cast("date"))
      return unity_catalog_df
    except originatorError as e:
      print(f"Error: {e.message} given value: {e.value} Given Originator does not exits in this template.(constants.py)")
    except eventError as e:
      print(f"Error: {e.message} given value: {e.value} Given Event does not exits in this template.(constants.py)")
    except stackError as e:
      print(f"Error: {e.message} given value: {e.value} Given stack does not exits in this template.(constants.py)")
    except Exception as e:
      print(f"An error occurred: {str(e)}")

  
  
  
