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
    self.error_columns = ["eventDetailError", "originatorDetailError", "shadowEventNotificationError"]
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
    # Critical null check for input DataFrame
    if df is None:
      print("Error: Input DataFrame is None. Cannot proceed with validation.")
      return

    self.set_schemaError()
    # self.widget.getErrorMessage()
    self.payloads.clear() #clearing payloads dict
    self.schemaReport.clear() #clearing payloads dict
    self.filter_productFamilies = self.products

    # Safe widget access
    try:
      stack_type = self.widget.getStackType()
      print(f"In {stack_type}: ")
    except:
      print("In Unknown Stack: ")

    print(f"Comprehensive Schema Validation Analysis")
    print(f"Error Pattern: '{self.schemaError}' (empty means any error)")
    print(f"Validating columns: {', '.join(self.error_columns)}\n")
    
    if((self.is_allPlatormFamilies)|(self.filter_productFamilies==[''])):
      self.providePayload=False                  

    if filterExp is not None:
      df = df.filter(filterExp)

    '''ToDo: need to handle a case when platform family is not selected in widget'''
    # if filter_productFamilies is None:
    #   filter_productFamilies = df.select("platform_family").distinct().collect()[0]
    #     # display(df.select("platform_family").distinct())
    
    for productFamily in self.filter_productFamilies:
      try:
        productDf = df.filter(col("platform_family").rlike(productFamily))
        if productDf is None:
          print(f"Error: Failed to filter data for {productFamily}\n")
          continue

        productTotalEvents = productDf.count()

        if productTotalEvents==0:
          print(f"no data found for {productFamily} for {filterExp}\n")
          continue
        else:
          # Perform comprehensive error analysis for all error types
          error_analysis, total_events = self.analyze_errors_by_type(productDf, productFamily)
          if error_analysis is None:
            print(f"Error: Failed to analyze errors for {productFamily}\n")
            continue

          self.print_detailed_error_analysis(error_analysis, total_events, productFamily)

          # Use combined error condition for further processing
          productSchemaErrorDf = productDf.filter(self.get_combined_error_filter_condition())
          if productSchemaErrorDf is None:
            print(f"Error: Failed to filter error data for {productFamily}\n")
            continue

          schemaErrorEvents = productSchemaErrorDf.count()
          if(schemaErrorEvents>0):
            # Safe collection access for JSON schema
            try:
              schema_result = productSchemaErrorDf.select(schema_of_json(col("rawJson")).alias("json_schema")).collect()
              if schema_result and len(schema_result) > 0 and schema_result[-1] is not None:
                json_schema = schema_result[-1]['json_schema']
              else:
                print(f"Warning: No valid JSON schema found for {productFamily}, skipping schema parsing")
                continue
            except Exception as e:
              print(f"Error extracting JSON schema for {productFamily}: {str(e)}")
              continue
            try:
              productDf=productDf.withColumn('ParsedRawJson',from_json(col('rawJson'),json_schema))
              productSchemaErrorDf  = productDf.filter(self.get_combined_error_filter_condition())

              # Safe column path finding
              notification_paths = self.find_column_paths(productDf,"notificationTrigger")
              if notification_paths and len(notification_paths) > 0 and "Missing field 'version'" not in self.schemaError:
                productDf=productDf.withColumn('notificationTrigger',coalesce(productDf.event.eventDetail.notificationTrigger,productDf.ParsedRawJson.event.eventDetail.notificationTrigger))

              reportCols = self.get_columnReqInSchemaValidnReport(productDf)
              if reportCols is None:
                print(f"Error: Failed to get report columns for {productFamily}")
                continue
            except Exception as e:
              print(f"Error processing DataFrame for {productFamily}: {str(e)}")
              continue
      
            try:
              totalEventsDf = productDf.groupBy(reportCols).count().withColumnRenamed("count","totalEvents").withColumnRenamed("firmwareVersion","fw").withColumnRenamed("platform_family","product")
              if totalEventsDf is None:
                print(f"Error: Failed to create total events DataFrame for {productFamily}")
                continue

              if "notificationTrigger" in totalEventsDf.columns:
                totalEventsDf = totalEventsDf.withColumnRenamed("notificationTrigger","triggerType")

              schemaErrorEventsDf = productDf.filter(self.get_combined_error_filter_condition()).groupBy(reportCols).count().withColumnRenamed("count","schemaErrorEvents")
              if schemaErrorEventsDf is None:
                print(f"Error: Failed to create error events DataFrame for {productFamily}")
                continue

              if joinType is None:
                joinType = "left"

              joinType_conditions = (schemaErrorEventsDf["firmwareVersion"]==totalEventsDf["fw"])&(schemaErrorEventsDf["platform_family"]==totalEventsDf["product"])
              if "notificationTrigger" in schemaErrorEventsDf.columns:
                joinType_conditions = joinType_conditions & (schemaErrorEventsDf["notificationTrigger"]==totalEventsDf["triggerType"])

              schemaReportDf = schemaErrorEventsDf.join(totalEventsDf,joinType_conditions,joinType).withColumn("schemaError%",format_number(((col("schemaErrorEvents")/col("totalEvents"))*100),2)).drop("fw","product")
              if schemaReportDf is None:
                print(f"Error: Failed to create schema report DataFrame for {productFamily}")
                continue

              if  "notificationTrigger" in schemaErrorEventsDf.columns:
                schemaReportDf = schemaReportDf.drop("triggerType")

              # store all reports df into a dict: key-> platform_family_name, value->report_df_filtered_for_respected_platform_family
              self.schemaReport[productFamily]=schemaReportDf
            except Exception as e:
              print(f"Error creating report DataFrames for {productFamily}: {str(e)}")
              continue
          
            # store all payloads df into a dict
            if (schemaErrorEvents>0) and (self.providePayload is True):
              try:
                # Filter for records with no errors in any of the three error columns
                errorFree_df=productDf.filter(
                  (col("eventDetailError").isNull()) &
                  (col("originatorDetailError").isNull()) &
                  (col("shadowEventNotificationError").isNull())
                )
                if errorFree_df is not None:
                  payload_result = self.getSameDeviceExpectedPayload(productSchemaErrorDf,errorFree_df)
                  if payload_result is not None:
                    self.payloads[productFamily] = payload_result
                  else:
                    print(f"Warning: Failed to generate payloads for {productFamily}")
                else:
                  print(f"Warning: No error-free data found for {productFamily}")
              except Exception as e:
                print(f"Error generating payloads for {productFamily}: {str(e)}")
      except Exception as e:
        print(f"Error processing product family {productFamily}: {str(e)}")
        continue
    return

  def set_schemaError(self):
    try:
      error_msg = self.widget.getErrorMessage()
      self.schemaError = error_msg if error_msg is not None else ""
    except Exception as e:
      print(f"Warning: Could not get error message from widget: {str(e)}")
      self.schemaError = ""

  def get_error_filter_condition(self, error_column):
    """
    Creates filter condition for a specific error column based on schema error message.
    Returns a PySpark column condition for filtering error records.
    """
    if self.schemaError == "":
      # If no specific error message, filter for any non-null error in the column
      return col(error_column).isNotNull()
    else:
      # If specific error message provided, filter for that message in the column
      return col(error_column).rlike(self.schemaError)

  def get_combined_error_filter_condition(self):
    """
    Creates a combined filter condition for all three error columns.
    Returns a PySpark column condition that matches any of the three error types.
    """
    conditions = []
    for error_col in self.error_columns:
      conditions.append(self.get_error_filter_condition(error_col))

    # Combine all conditions with OR logic
    combined_condition = conditions[0]
    for condition in conditions[1:]:
      combined_condition = combined_condition | condition

    return combined_condition

  def analyze_errors_by_type(self, df, product_family):
    """
    Analyzes errors by type for a specific product family.
    Returns a dictionary with error counts for each error type.
    """
    if df is None:
      print(f"Error: DataFrame is None for {product_family}")
      return None, 0

    try:
      error_analysis = {}
      total_events = df.count()

      for error_col in self.error_columns:
        try:
          if self.schemaError == "":
            error_df = df.filter(col(error_col).isNotNull())
          else:
            error_df = df.filter(col(error_col).rlike(self.schemaError))

          if error_df is not None:
            error_count = error_df.count()
            error_percentage = (error_count / total_events * 100) if total_events > 0 else 0
          else:
            error_count = 0
            error_percentage = 0

          error_analysis[error_col] = {
            'count': error_count,
            'percentage': error_percentage
          }
        except Exception as e:
          print(f"Error analyzing {error_col} for {product_family}: {str(e)}")
          error_analysis[error_col] = {'count': 0, 'percentage': 0}

      # Combined analysis (any error type)
      try:
        combined_error_df = df.filter(self.get_combined_error_filter_condition())
        if combined_error_df is not None:
          combined_count = combined_error_df.count()
          combined_percentage = (combined_count / total_events * 100) if total_events > 0 else 0
        else:
          combined_count = 0
          combined_percentage = 0
      except Exception as e:
        print(f"Error in combined analysis for {product_family}: {str(e)}")
        combined_count = 0
        combined_percentage = 0

      error_analysis['combined'] = {
        'count': combined_count,
        'percentage': combined_percentage
      }

      return error_analysis, total_events
    except Exception as e:
      print(f"Error in analyze_errors_by_type for {product_family}: {str(e)}")
      return None, 0

  def print_detailed_error_analysis(self, error_analysis, total_events, product_family):
    """
    Prints detailed error analysis for all error types.
    """
    print(f'=== Error Analysis for {product_family} ===')
    print(f'Total Events: {total_events}')
    print(f'Search Pattern: "{self.schemaError}" (empty means any error)\n')

    # Print individual error type analysis
    for error_col in self.error_columns:
      analysis = error_analysis[error_col]
      print(f'{error_col}:')
      print(f'  - Error Events: {analysis["count"]}')
      print(f'  - Error Percentage: {analysis["percentage"]:.2f}%')

    # Print combined analysis
    combined = error_analysis['combined']
    print(f'\nCombined (Any Error Type):')
    print(f'  - Error Events: {combined["count"]}')
    print(f'  - Error Percentage: {combined["percentage"]:.2f}%')
    print('=' * 50 + '\n')

  def print_reproducibilty_schemaValidationIssue(self,errorCount = 0,totalCount = 0,product = ""):
    print(f'Total Events for {product if product else "all product families"}:{totalCount}')
    print(f'Schema error Events for {product}:{errorCount} ')
    print(f'schema error percentage for {product}: {format((errorCount/totalCount)*100,".2f")}\n')

  def getSameDeviceExpectedPayload(self, errorDf, non_errorDf):
    if errorDf is None or non_errorDf is None:
      print("Error: One or both DataFrames are None in getSameDeviceExpectedPayload")
      return None

    try:
      filter_paths = self.find_column_paths(errorDf,"filterType")
      if filter_paths and len(filter_paths) > 1:
        errorDf = errorDf.filter(col("ParsedRawJson.event.filter.filterType")=="inclusion")
      else:
        print("Note: All Events Found With FilterType/Filter field as Null\n")
        
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
    except Exception as e:
      print(f"Error in getSameDeviceExpectedPayload: {str(e)}")
      return None

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
          print(f"Payloads for {product} - Comprehensive Error Analysis")
          print(f"Error types analyzed: {', '.join(self.error_columns)}")
          print(f"Search pattern: '{self.schemaError}'\n")
          for payload_type,payload in allTypePayloads.items():
            if self.is_validPayload(payload):
              print(f"Here is the {payload_type} of {product}:")
              display_function(payload)

  def display_reports(self,display_function):
    if(len(self.schemaReport)==0):
      print("No Error Found")
    else:
      for product, report in self.schemaReport.items():
        print(f"Comprehensive Schema Validation Report for {product}")
        print(f"(Combined analysis of {', '.join(self.error_columns)})")
        display_function(report)

  def display_detailed_error_breakdown(self, display_function):
    """
    Displays detailed breakdown of errors by type for each product family.
    This provides individual analysis for each error column.
    """
    if(len(self.schemaReport)==0):
      print("No Error Found")
      return

    print("=== DETAILED ERROR BREAKDOWN BY TYPE ===\n")

    for product_family in self.filter_productFamilies:
      print(f"Product Family: {product_family}")
      print("-" * 40)

      # Load the product data again for detailed analysis
      try:
        # This is a simplified approach - in a real implementation,
        # you might want to store the detailed analysis results
        print(f"For detailed breakdown by error type, re-run analysis with specific error patterns.")
        print(f"Error columns analyzed: {', '.join(self.error_columns)}")
        print(f"Current search pattern: '{self.schemaError}'\n")
      except Exception as e:
        print(f"Error generating detailed breakdown for {product_family}: {str(e)}\n")
