from schemaValidLib.generic.common_util import Common_Utils
from pyspark.sql.functions import col, schema_of_json, from_json, coalesce, format_number, struct
from pyspark.sql import DataFrame

class Utils(Common_Utils):
  def __init__(self)->None:
    super().__init__()
    self.providePayload = True
    self.payloads = {}
    self.schemaReport={}
    self.schemaError = ""
    self.filter_productFamilies = ""
    self.common_columns = [
      "productNumber",
      "event.eventDetailType",
      "originator.originatorDetailType",
      "originator.originatorDetail.firmwareVersion",
      "originator.originatorDetail.deviceUuid",
      "originator",
      "eventDetailError",
      "shadowEventNotificationError",
      "originatorDetailError",
      "event",
      "event.dateTime",
      "eventMeta.eventMetaDetail.payloadId",
      "eventMeta",
      "rawJson"]
    
  """
  This function exracts selectives columns from the overall df.
  param-1: df-> pass a dataframe having overall records without any filters or anything else.
  param-2: otherColumns-> pass the list of columns that you want include in your final schema validation report.

  return param is dataframe with all selective columns that you need in your final report.
  """
  def get_extractData(self, df, otherColumns=None):
    df = self.segrate_productNumber(df)
    df = self.add_fwVerLineCol(df,self.get_sampleFw(df)) 

    if otherColumns is not None:
      self.common_columns.extend(cols for cols in otherColumns)
    df = self.combine_ref(df.select(self.common_columns))
    return df

  """
  this function returns the final report for the events having schema validations
  param-1: df-> dataframe having all events for the req. originator and event gun with defined stack env for a defined timeperiod
  param-2: env-> name of stack--> used only to denote in output, for which stack the report is generated.
  param-3: productFamilies(optional)-> list of all products for what you want require report; ex-["NOVELLI","VASARI"]
  param-4: schemaError(optional)-> sample error message 
  param-5: otherColumns(optional)-> columns need to be include in final report
  param-6: filterExp(optional)-> query that need to be filtered from records for exactly required report, ex: filter by firmwareverion---> filterExp=(col("firmwareVersion").rlike("6.17.3.115")))
  """
  def getSchemaValidationProcessed(self,df,otherColumns=[],filterExp=None,joinType=None):
    self.set_schemaError()
    # self.widget.getErrorMessage()
    self.payloads.clear() #clearing payloads dict
    self.schemaReport.clear() #clearing payloads dict
    self.filter_productFamilies = self.products
    print(f"In {self.widget.getStackType()}: ")
    print(f"For Error {self.schemaError}:\n")
    
    if((self.is_allPlatormFamilies)|(self.filter_productFamilies==[''])):
      self.providePayload=False                  

    if filterExp is not None:
      df = df.filter(filterExp)

    '''ToDo: need to handle a case when platform family is not selected in widget'''
    # if filter_productFamilies is None:
    #   filter_productFamilies = df.select("platform_family").distinct().collect()[0]
    #     # display(df.select("platform_family").distinct())
    
    for productFamily in self.filter_productFamilies:
      productDf = df.filter(col("platform_family").rlike(productFamily))
      productTotalEvents = productDf.count()

      if productTotalEvents==0:
        print(f"no data found for {productFamily} for {filterExp}\n")
        continue
      else:
        if self.schemaError == "":
          productSchemaErrorDf = productDf.filter(col("eventDetailError").isNotNull())
        else:
          productSchemaErrorDf = productDf.filter(col("eventDetailError").rlike(self.schemaError))
        schemaErrorEvents = productSchemaErrorDf.count()
        self.print_reproducibilty_schemaValidationIssue(errorCount=schemaErrorEvents,totalCount=productTotalEvents,product=productFamily)
        if(schemaErrorEvents>0):
          json_schema = productSchemaErrorDf.select(schema_of_json(col("rawJson")).alias("json_schema")).collect()[-1]['json_schema']
          productDf=productDf.withColumn('ParsedRawJson',from_json(col('rawJson'),json_schema))
          productSchemaErrorDf  = productDf.filter(col("eventDetailError").rlike(self.schemaError))
          if(len(self.find_column_paths(productDf,"notificationTrigger"))!=0 and "Missing field 'version'" not in self.schemaError):
            productDf=productDf.withColumn('notificationTrigger',coalesce(productDf.event.eventDetail.notificationTrigger,productDf.ParsedRawJson.event.eventDetail.notificationTrigger))
          reportCols = self.get_columnReqInSchemaValidnReport(productDf)
      
          totalEventsDf = productDf.groupBy(reportCols).count().withColumnRenamed("count","totalEvents").withColumnRenamed("firmwareVersion","fw").withColumnRenamed("platform_family","product")

          if "notificationTrigger" in totalEventsDf.columns:
            totalEventsDf = totalEventsDf.withColumnRenamed("notificationTrigger","triggerType")
          schemaErrorEventsDf = productDf.filter(col("eventDetailError").rlike(self.schemaError)).groupBy(reportCols).count().withColumnRenamed("count","schemaErrorEvents")
          if joinType is None:
            joinType = "left"

          joinType_conditions = (schemaErrorEventsDf["firmwareVersion"]==totalEventsDf["fw"])&(schemaErrorEventsDf["platform_family"]==totalEventsDf["product"])
          if "notificationTrigger" in schemaErrorEventsDf.columns:
            joinType_conditions = joinType_conditions & (schemaErrorEventsDf["notificationTrigger"]==totalEventsDf["triggerType"]) 
          schemaReportDf = schemaErrorEventsDf.join(totalEventsDf,joinType_conditions,joinType).withColumn("schemaError%",format_number(((col("schemaErrorEvents")/col("totalEvents"))*100),2)).drop("fw","product")
          if  "notificationTrigger" in schemaErrorEventsDf.columns:
            schemaReportDf = schemaReportDf.drop("triggerType")

          # store all reports df into a dict: key-> platform_family_name, value->report_df_filtered_for_respected_platform_family
          self.schemaReport[productFamily]=schemaReportDf
          
          # store all payloads df into a dict
          if (schemaErrorEvents>0) and (self.providePayload is True):
            errorFree_df=productDf.filter(col("eventDetailError").isNull())
            self.payloads[productFamily] = self.getSameDeviceExpectedPayload(productSchemaErrorDf,errorFree_df)
    return

  def set_schemaError(self):
    self.schemaError = self.widget.getErrorMessage()

  def print_reproducibilty_schemaValidationIssue(self,errorCount = 0,totalCount = 0,product = ""):
    print(f'Total Events for {product if product else "all product families"}:{totalCount}')
    print(f'Schema error Events for {product}:{errorCount} ')
    print(f'schema error percentage for {product}: {format((errorCount/totalCount)*100,".2f")}\n')

  def getSameDeviceExpectedPayload(self, errorDf, non_errorDf):

    if len(self.find_column_paths(errorDf,"filterType"))>1:
      errorDf = errorDf.filter(col("ParsedRawJson.event.filter.filterType")=="inclusion")
    else:
      print("Note: All Events Found Wth FilerType/ Fiter field as Null\n")

    errorDf = errorDf.withColumn(
      "error_printer",
      struct(
        col("originator.originatorDetail.deviceUuid").alias("deviceUuid"),
        col("originator.originatorDetail.firmwareVersion").alias("firmwareVersion"),
        col("dateTime").alias("dateTime"),
        col("originator.originatorDetail.currentDateTime").alias("originator_originatorDetail_currentDateTime"),
        col("event.dateTime").alias("event_dateTime"),
        col("eventMeta.eventMetaDetail.dateTime").alias("eventMeta_eventMetaDetail_dateTime")
      )
    ).withColumnRenamed("rawJson","errorPayload").select("error_printer","errorPayload")
    
    non_errorDf = non_errorDf.filter(col("event.filter.filterType")=="inclusion")\
      .withColumn("printer",struct(
        col("originator.originatorDetail.deviceUuid").alias("deviceUuid"),
        col("originator.originatorDetail.firmwareVersion").alias("firmwareVersion"),
        col("originator.originatorDetail.currentDateTime").alias("originator_originatorDetail_currentDateTime"),
        col("event.dateTime").alias("event_dateTime"),
        col("eventMeta.eventMetaDetail.dateTime").alias("eventMeta_eventMetaDetail_dateTime")
      )
    )\
    .filter((col("eventDetailError").isNull())&(col("originatorDetailError").isNull())&(col("shadowEventNotificationError").isNull()))\
    .withColumn("payload",struct(col("event"),col("eventMeta"),col("originator"))).select("printer","payload")

    expectedPayloadDf = errorDf.join(non_errorDf,(non_errorDf["printer.deviceUuid"]==errorDf["error_printer.deviceUuid"])&(non_errorDf["printer.firmwareVersion"]==errorDf["error_printer.firmwareVersion"])&((non_errorDf["printer.event_dateTime"]==errorDf["error_printer.event_dateTime"])|(non_errorDf["printer.originator_originatorDetail_currentDateTime"]==errorDf["error_printer.originator_originatorDetail_currentDateTime"])|(non_errorDf["printer.eventMeta_eventMetaDetail_dateTime"]==errorDf["error_printer.eventMeta_eventMetaDetail_dateTime"])),"inner")

    payloads = {"error_payload":errorDf,"expected_payload":non_errorDf,"compared_payload":expectedPayloadDf}
    return payloads

  def is_validPayload(self, payloads):
    if isinstance(payloads, DataFrame) and payloads.count() > 0:
      return True

  def get_columnReqInSchemaValidnReport(self, df):
    alwaysReqCol = [col("platform_family"),col("firmwareVersion")]
    isNotificationTriggerCol_found = self.find_column_paths(df,"notificationTrigger")
    if(len(isNotificationTriggerCol_found)>0 and "Missing field 'version'" not in self.schemaError):
      alwaysReqCol.append(col("notificationTrigger"))
    return alwaysReqCol
  
  def display_payloads(self,display_function):
    if(len(self.payloads)==0):
      print("No Error Found")
    else:
      for product, allTypePayloads in self.payloads.items():
        generate_payloads=input(f"Do you require Payloads for {product} (Yes or No): ")
        if(generate_payloads.upper()=="YES"):
          for payload_type,payload in allTypePayloads.items():
            if self.is_validPayload(payload):
              print(f"Here is the {payload_type} of {product} for {self.schemaError}:")
              display_function(payload)

  def display_reports(self,display_function):
    if(len(self.schemaReport)==0):
      print("No Error Found")
    else:
      for product, report in self.schemaReport.items():
        print(f"Schema Validation Report for {product}")
        display_function(report)