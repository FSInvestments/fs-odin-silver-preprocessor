# Databricks notebook source
def pullSmartsheetPreProcessorSheets(smart_sheet_access_token, tgtEnv, excludeTaxos = True):
  """
  Creates each SmartSheet in the ODIN_REFERENCE foldar as a DF. Also creates DFs of the config and mapping tables for the self maintained REF tables
  """
  ## For config/mapping sheets saved by sheetID, use non-prod vs. prod depending on tgtEnv
  if tgtEnv == 'dev' or tgtEnv == 'qa':
    for key in ['config-preprocessor-split-data-non-production']:
      smart_sheet_id = dbutils.secrets.get(scope = "fs-smartsheet", key = key)
      keyName = key.rstrip('-non-production')
      preProcessorLocal["{}_DF".format(keyName)] = getSmartSheetDf(smart_sheet_access_token, smart_sheet_id)
    ## Request for these keys to be made with non-production secrets not yet filled, add to above once done ##
    preProcessorLocal['config-preprocessor-link-ref_DF'] = getSmartSheetDf(smart_sheet_access_token, '1031182105241476')
    preProcessorLocal['map-preprocessor-link-ref_DF'] = getSmartSheetDf(smart_sheet_access_token, '6316122183100292')
    preProcessorLocal['config-preprocessor-hub-tables_DF'] = getSmartSheetDf(smart_sheet_access_token, '2094978563368836')
  ## PRODUCTION KEYS ##
  else:
    for key in ["config-preprocessor-hub-tables", "config-preprocessor-link-ref", "config-preprocessor-split-data", "map-preprocessor-link-ref"]:
      smart_sheet_id = dbutils.secrets.get(scope = "fs-smartsheet", key = key)
      preProcessorLocal["{}_DF".format(key)] = getSmartSheetDf(smart_sheet_access_token, smart_sheet_id)
  
  ## REF sheets stored in ODIN_REFERENCE workspace ##
  ## Use non-prod folder if dev or qa run ##
  if tgtEnv == 'dev' or tgtEnv == 'qa':
    folderID = dbutils.secrets.get(scope = "fs-smartsheet", key = "folder-preprocessor-ref-tables-non-production")
  ## Use prod folder elsewise ##
  else:
    folderID = dbutils.secrets.get(scope = "fs-smartsheet", key = "folder-preprocessor-ref-tables")
  ## Pull all sheet IDs and names from the folder ##
  requestHeaders = {"Authorization":"Bearer {}".format(smart_sheet_access_token), 
              "Content-Type":"application/json"}
  jsonResponse = requests.get("https://api.smartsheet.com/2.0/folders/{}".format(folderID), headers = requestHeaders).json()
  sheetsInFolderDict = {x["name"]: x["id"] for x in jsonResponse["sheets"]}
  
  ## Exclude Taxonomy tables from the run ##
  ## If we decide to maintain TAXO tables in the same process, make excludeTaxos param false
  ## If to be maintained elsewhere, can delete this IF statement and delete any TAXO_ tables from the smartsheet folder
  if excludeTaxos == True:
    print("Excluded taxonomy tables from Smartsheets")
    sheetsInFolderDict = {x: y for x, y in sheetsInFolderDict.items() if x.startswith('REF_')}  
   
  ## Add sheets from folder to local dictionaries ##
  for key, value in sheetsInFolderDict.items():
    smart_sheet_id = str(value)
    refSheetIds["{}_DF".format(key)] = smart_sheet_id
    preProcessorLocal["{}_DF".format(key)] = getSmartSheetDf(smart_sheet_access_token, smart_sheet_id)

# COMMAND ----------

def create_reference_pivot_views(bronzeDB, all_exceptions, tgtEnv, metricsDF):
  '''
  Create pivoted tables to use in reference process. Specifically usedf for flattening out EDM_MASTER_SEC_ALTERNATIVE_EXTRACT
  '''
  startTime = datetime.datetime.now()
  
  ## Load Mapping smartsheet ##
  mappingDF = preProcessorLocal["map-preprocessor-link-ref_DF"]
  
  ## Bronze table has incorrect name in production env ##
  if bronzeDB.lower() == 'odin_invops_bronze':
    issuerAltIDTable = 'edm_master_sec_issuer_alternative_id_extract_'
  else:
    issuerAltIDTable = 'edm_master_sec_issuer_alternative_id_extract'
    
  try: 
    ## INSTRUMENT ##
    ## Get list of ID types ##
    edmAltIDTypes = spark.sql("SELECT DISTINCT ID_TYPE FROM {}.edm_master_sec_alternative_id_extract".format(bronzeDB)).collect()
    cleanedInstIdTypes = [x['ID_TYPE'].strip(' ') for x in  edmAltIDTypes]
    # Filter for only fields in the mapping
    instBronzeFieldsCollect = mappingDF.filter(col("ENTITY") == 'INSTRUMENT').select(col('bronzeField')).collect()
    instBronzeFields = [x['bronzeField'] for x in instBronzeFieldsCollect]
    filteredInstIdTypes = [f for f in cleanedInstIdTypes if f in instBronzeFields]
    
    ## Get max date and other metadata ##
    max_edm_alt_id_date = spark.sql("SELECT max(LastLoadDts) AS maxLastLoadDts FROM {}.edm_master_sec_alternative_id_extract".format(bronzeDB)).head().maxLastLoadDts
    metaDF = spark.sql("""SELECT FileDerivedDate, FileOriginationTimestamp, FileName, FileProvider FROM {}.edm_master_sec_alternative_id_extract WHERE to_timestamp(LastLoadDts) = to_timestamp('{}')""".format(bronzeDB, max_edm_alt_id_date))
    FileDerivedDate = metaDF.head().FileDerivedDate         
    FileOriginationTimestamp = metaDF.head().FileOriginationTimestamp
    FileName = metaDF.head().FileName
    FileProvider = metaDF.head().FileProvider

    ## Create a dataframe for each ID type ##
    idTypeDfs = {}
    for edmID in filteredInstIdTypes:
      if edmID != '':
        idTypeDfs['{}_DF'.format(edmID)] = spark.sql("""SELECT DISTINCT EDM_SEC_ID, ID_VALUE AS {idType}
                                                        FROM {bronzeDB}.edm_master_sec_alternative_id_extract
                                                        WHERE ID_TYPE = '{idType}'
                                                        AND ACTIVE <> 'False'
                                                        AND to_timestamp(LastLoadDts) = to_timestamp('{maxDate}')
                                                    """.format(idType = edmID, bronzeDB = bronzeDB, maxDate = max_edm_alt_id_date))

    ## Get the entire list of EDM_SEC_IDS that exist ##
    masterEDMTable = spark.sql("SELECT DISTINCT EDM_SEC_ID FROM {}.edm_master_sec_extract".format(bronzeDB))
    ## Join each ID Type DF to the master list ##
    for df in idTypeDfs.values():
      masterEDMTable = masterEDMTable.join(df, 'EDM_SEC_ID', 'left')

    ## Add meta data to the full view ##
    masterEDMTable = masterEDMTable.withColumn('FileDerivedDate', lit(FileDerivedDate))
    masterEDMTable = masterEDMTable.withColumn('FileOriginationTimestamp', lit(FileOriginationTimestamp))
    masterEDMTable = masterEDMTable.withColumn('FileName', lit(FileName))
    masterEDMTable = masterEDMTable.withColumn('FileProvider', lit(FileProvider))
    masterEDMTable = masterEDMTable.withColumn('LastLoadDts', lit(max_edm_alt_id_date))

    ## BROADRIDGE JOIN. TO BE DELETED ONCE BIMS IDs ADDED TO EDM ##
    broadridgeInstruments = spark.sql("""SELECT m.EDM_SEC_ID, m.MasterSecID
                                         FROM {db}.mainextract_cm m
                                         GROUP BY m.EDM_SEC_ID, m.MasterSecID, m.LastLoadDts
                                         HAVING m.LastLoadDts = (select max(i.LastLoadDts) from {db}.mainextract_cm i where i.EDM_SEC_ID = m.EDM_SEC_ID)
                                      """.format(db = bronzeDB))

    EDMandBIMSInstrument = masterEDMTable.join(broadridgeInstruments, 'EDM_SEC_ID', 'left')
    EDMandBIMSInstrument = EDMandBIMSInstrument.withColumn('LastLoadDts', current_timestamp())
    EDMandBIMSInstrument.createOrReplaceTempView('edm_master_sec_alternative_id_extract_PIVOTED')

    ## ISSUER ##
    ## Get list of ID types ##
    edmAltIssuerIDTypes = spark.sql("SELECT DISTINCT ID_TYPE FROM {}.{}".format(bronzeDB, issuerAltIDTable)).collect()
    cleanedIssIdTypes = [x['ID_TYPE'].strip(' ') for x in  edmAltIssuerIDTypes]
    # Filter for only fields in the mapping
    issBronzeFieldsCollect = mappingDF.filter(col("ENTITY") == 'ISSUER').select(col('bronzeField')).collect()
    issBronzeFields = [x['bronzeField'] for x in issBronzeFieldsCollect]
    filteredIssIdTypes = [f for f in cleanedIssIdTypes if f in issBronzeFields]
    
    
    ## Get max date and other metadata ##
    max_edm_issuer_alt_id_date = spark.sql("SELECT max(LastLoadDts) AS maxLastLoadDts FROM {}.{}".format(bronzeDB, issuerAltIDTable)).head().maxLastLoadDts
    issuerMetaDF = spark.sql("""SELECT FileDerivedDate, FileOriginationTimestamp, FileName, FileProvider 
                                FROM {}.{} WHERE to_timestamp(LastLoadDts) = to_timestamp('{}')""".format(bronzeDB, issuerAltIDTable, max_edm_issuer_alt_id_date))    
    IssuerFileDerivedDate = issuerMetaDF.head().FileDerivedDate         
    IssuerFileOriginationTimestamp = issuerMetaDF.head().FileOriginationTimestamp
    IssuerFileName = issuerMetaDF.head().FileName
    IssuerFileProvider = issuerMetaDF.head().FileProvider

    ## Create a dataframe for each ID type ##
    idIssuerTypeDfs = {}
    for issuerID in filteredIssIdTypes:
      if issuerID != '':
        idIssuerTypeDfs['{}_DF'.format(issuerID)] = spark.sql("""SELECT DISTINCT EDM_PTY_ID, ID_VALUE AS {idType}
                                                        FROM {bronzeDB}.{bronzeTbl}
                                                        WHERE ID_TYPE = '{idType}'
                                                        AND ACTIVE <> 'False'
                                                        AND to_timestamp(LastLoadDts) = to_timestamp('{maxDate}')
                                                        """.format(idType = issuerID, bronzeDB = bronzeDB, maxDate = max_edm_issuer_alt_id_date, bronzeTbl = issuerAltIDTable))


    ## Get the entire list of EDM_PTY_IDS that exist ##
    masterEDMIssuerTable = spark.sql("SELECT DISTINCT EDM_PTY_ID FROM {}.edm_master_sec_issuer_extract".format(bronzeDB))
    ## Join each ID Type DF to the master list ##
    for df in idIssuerTypeDfs.values():
      masterEDMIssuerTable = masterEDMIssuerTable.join(df, 'EDM_PTY_ID', 'left')

    ## Add meta data to the full view ##
    masterEDMIssuerTable = masterEDMIssuerTable.withColumn('FileDerivedDate', lit(IssuerFileDerivedDate))
    masterEDMIssuerTable = masterEDMIssuerTable.withColumn('FileOriginationTimestamp', lit(IssuerFileOriginationTimestamp))
    masterEDMIssuerTable = masterEDMIssuerTable.withColumn('FileName', lit(IssuerFileName))
    masterEDMIssuerTable = masterEDMIssuerTable.withColumn('FileProvider', lit(IssuerFileProvider))
    masterEDMIssuerTable = masterEDMIssuerTable.withColumn('LastLoadDts', lit(max_edm_issuer_alt_id_date))

    ## BROADRIDGE JOIN. TO BE DELETED ONCE BIMS IDs ADDED TO EDM ##
    broadridgeIssuers = spark.sql("""SELECT l.EDM_PTY_ID, l.LegalEntityID
                                         FROM {db}.leextract l
                                         GROUP BY l.EDM_PTY_ID, l.LegalEntityID, l.LastLoadDts
                                         HAVING l.LastLoadDts = (select max(i.LastLoadDts) from {db}.leextract i where i.EDM_PTY_ID = l.EDM_PTY_ID)
                                      """.format(db = bronzeDB))
    EDMandBIMSIssuer = masterEDMIssuerTable.join(broadridgeIssuers, 'EDM_PTY_ID', 'left')
    EDMandBIMSIssuer = EDMandBIMSIssuer.withColumn('LastLoadDts', current_timestamp())
    EDMandBIMSIssuer.createOrReplaceTempView('edm_master_sec_issuer_alternative_id_extract_PIVOTED')
    
    ## Write to metrics table ##
    runTime = datetime.datetime.now() - startTime
    metricsDF = appendToMetricsDF(metricsDF = metricsDF,
                                        lastLoadDts = "'{}'".format("null"), 
                                        FAILED = False,
                                        PHASE = "'{}'".format("PRE_PROCESS"), 
                                        SOURCE = "'{}'".format("null"), 
                                        TABLENAME = "'{}'".format('EDM_IDS_PIVOT'),
                                        FILENAME = "'{}'".format("null"), 
                                        PROCESSING_TIME = "'{}'".format(runTime), 
                                        RECORDS_READ = "'{}'".format("null"), 
                                        DUP_RECORDS = "'{}'".format("null"), 
                                        QUARANTINED_RECORDS = "'{}'".format("null"), 
                                        EXCEPTIONS = "'{}'".format("null"), 
                                        RECORDS_WRITTEN = "'{}'".format("null"))
    print('Created EDM and EDM_ISSUER ALT_ID PIVOTS')  
  except:
    print("Failed to create EDM and EDM_ISSUER ALT_ID PIVOTS")
    ## Post to exceptions DF ##
    exceptionVariables = {"FS_POST_ID":"00009", 
                                "ORIGIN":'edm_alt_id_pivots',
                                "EXCEPTION_ID_TYPE":"Code Failure", 
                                "FILE_CREATION_DTS":None, 
                                "FOR_SUBJECT":None, 
                                "FOR_BODY":None}
    all_exceptions = generateExceptionDf(exceptionVariables, tgtEnv = tgtEnv, all_exceptions = all_exceptions)
    ## Append to metrics ##
    runTime = datetime.datetime.now() -  startTime
    metricsDF = appendToMetricsDF(metricsDF = metricsDF,
                                        lastLoadDts = "'{}'".format("null"), 
                                        FAILED = True,
                                        PHASE = "'{}'".format("PRE_PROCESS"), 
                                        SOURCE = "'{}'".format("null"), 
                                        TABLENAME = "'{}'".format('EDM_IDS_PIVOT'),
                                        FILENAME = "'{}'".format("null"), 
                                        PROCESSING_TIME = "'{}'".format(runTime), 
                                        RECORDS_READ = "'{}'".format("null"), 
                                        DUP_RECORDS = "'{}'".format("null"), 
                                        QUARANTINED_RECORDS = "'{}'".format("null"), 
                                        EXCEPTIONS = "'{}'".format("null"), 
                                        RECORDS_WRITTEN = "'{}'".format("null"))
    
  return all_exceptions   

# COMMAND ----------

def split_bronze_files(referenceDB, bronzeDB, all_exceptions, metricsDF, tgtEnv):
  """
  There are some cases where we only get one file from a file provider but it provides two sorts of data. 
  An example of this is the KKR-FS All Assets file, where KKR sends us Instrument and Issuer data but its all on one file.
  This function creates or updates a new bronze table given the desired subset of the original file. For the KKR example,
  this would be the distinct IssuerID + applicable issuer data fields  
  
  For details on format_map and the Default class, see:  
  https://docs.python.org/3/library/stdtypes.html#str.format_map
  """
  splitStartTime = datetime.datetime.now()
  
  ## For format_map, see function doc strings for details ##
  class Default(dict):
    def __missing__(self, key):
        return key
  
  ## Load configs ##
  subFileConfig = preProcessorLocal["config-preprocessor-split-data_DF"].where("active == True").collect()
  ## For each file in configs##
  for sub in subFileConfig:
    try:
      sourceTable = sub["sourceTable"]
      destTable = sub["destTable"]
      sqlStatement = sub["sqlStatement"]
      mode = sub["mode"]
      destDB = sub["destDB"]
      if destDB == 'referenceDB':
        destDB = referenceDB
        LastLoadDtsColName = 'LAST_LOAD_DTS'
      elif destDB == 'bronzeDB':
        destDB = bronzeDB
        LastLoadDtsColName = 'LastLoadDts'
      else:
        pass
      
      tblList = spark.sql("show tables from {destDB}".format(destDB = destDB))
      tblListNames = [i['tableName'] for i in tblList.collect()]
      ## Max Date of sub_file table
      try:
        max_split_file_date = spark.sql("""SELECT max({lastLoadCol}) AS maxLastLoadDts 
                                          FROM {destDB}.{destTable}""".format(destDB = destDB, destTable = destTable, lastLoadCol = LastLoadDtsColName)).head().maxLastLoadDts
        if max_split_file_date == None:
          max_split_file_date = '1900-01-01T00:00:00.000+0000'
      except:
        max_split_file_date = '1900-01-01T00:00:00.000+0000'

      maxDateFormatted = "'{}'".format(max_split_file_date)
      sqlTxt = sqlStatement.format_map(Default(bronzeDB=bronzeDB, referenceDB=referenceDB, maxDate=maxDateFormatted))

      if mode == "merge":
        spark.sql(sqlTxt)
        print('Merged split-file: {}'.format(destTable))
        sourceDataAppendCount = 0
      else:
        ## Write to bronze table for sub-file ##
        sourceData = spark.sql(sqlTxt)
        if sourceData.count() == 0:
          sourceDataAppendCount = 0
          print('No new records past max_date for: {}'.format(destTable))
        else:
          if destTable in tblListNames:
            sourceAfterSchema = mainSchemaFunction(dbName = destDB, tblName = destTable, df = sourceData, enforceSchema = False)
            sourceDF = sourceAfterSchema.df
            sourceDF.write.format("delta").mode("append").saveAsTable("{destDB}.{destTable}".format(destDB = destDB, destTable = destTable))
            print('Wrote to split table: {}'.format(destTable))
            ## Count for metrics
            sourceDataAppendCount = sourceDF.count()
          else:
            sourceData.write.format("delta").mode("append").saveAsTable("{destDB}.{destTable}".format(destDB = destDB, destTable = destTable))
            ## Count for metrics
            sourceDataAppendCount = sourceData.count()

      ## Write to metrics table ##
      runTime = datetime.datetime.now() -  splitStartTime
      metricsDF = appendToMetricsDF(metricsDF = metricsDF,
                                          lastLoadDts = "'{}'".format("null"), 
                                          FAILED = False,
                                          PHASE = "'{}'".format("PRE_PROCESS"), 
                                          SOURCE = "'{}'".format("null"), 
                                          TABLENAME = "'{}'".format(destTable),
                                          FILENAME = "'{}'".format(sourceTable), 
                                          PROCESSING_TIME = "'{}'".format(runTime), 
                                          RECORDS_READ = "'{}'".format("null"), 
                                          DUP_RECORDS = "'{}'".format("null"), 
                                          QUARANTINED_RECORDS = "'{}'".format("null"), 
                                          EXCEPTIONS = "'{}'".format("null"), 
                                          RECORDS_WRITTEN = "'{}'".format(sourceDataAppendCount))
      print("Succesfully created split-file: {}".format(destTable))    
    except Exception as e:
      print("Failed creating split-file: {}".format(destTable))
      print(e)
      ## Post to exceptions DF ##
      exceptionVariables = {"FS_POST_ID":"00009", 
                                  "ORIGIN":destTable,
                                  "EXCEPTION_ID_TYPE":"Code Failure", 
                                  "FILE_CREATION_DTS":None, 
                                  "FOR_SUBJECT":None, 
                                  "FOR_BODY":None}
      all_exceptions = generateExceptionDf(exceptionVariables, tgtEnv = tgtEnv, all_exceptions = all_exceptions)
      ## Append to metrics ##
      runTime = datetime.datetime.now() -  splitStartTime
      metricsDF = appendToMetricsDF(metricsDF = metricsDF,
                                          lastLoadDts = "'{}'".format("null"), 
                                          FAILED = True,
                                          PHASE = "'{}'".format("PRE_PROCESS"), 
                                          SOURCE = "'{}'".format(bronzeDB), 
                                          TABLENAME = "'{}'".format(destTable),
                                          FILENAME = "'{}'".format(sourceTable), 
                                          PROCESSING_TIME = "'{}'".format(runTime), 
                                          RECORDS_READ = "'{}'".format("null"), 
                                          DUP_RECORDS = "'{}'".format("null"), 
                                          QUARANTINED_RECORDS = "'{}'".format("null"), 
                                          EXCEPTIONS = "'{}'".format("null"), 
                                          RECORDS_WRITTEN = "'{}'".format("null"))
    
  return all_exceptions    

# COMMAND ----------

def optimize_split_file_tables(all_exceptions, metricsDF, tgtEnv):
  ## Load configs ##
  tablesToOptimize = preProcessorLocal["config-preprocessor-split-data_DF"].where("active == True").select(col('destTable'), col('destDB')).distinct().collect()
  
  for table in tablesToOptimize:
    destDB = table['destDB']
    destTable = table['destTable']
    if destDB == 'referenceDB':
        destDB = referenceDB
        LastLoadDtsColName = 'LAST_LOAD_DTS'
    elif destDB == 'bronzeDB':
        destDB = bronzeDB
        LastLoadDtsColName = 'LastLoadDts'
    else:
      pass
    try:
      optimizeTbls(destDB,destTable)
    except Exception as e:
      print("Failed to optimize {}".format(destTable))
      print(e)
      ## Post to exceptions DF ##
      exceptionVariables = {"FS_POST_ID":"00009", 
                                  "ORIGIN":destTable,
                                  "EXCEPTION_ID_TYPE":"Code Failure", 
                                  "FILE_CREATION_DTS":None, 
                                  "FOR_SUBJECT":None, 
                                  "FOR_BODY":None}
      all_exceptions = generateExceptionDf(exceptionVariables, tgtEnv = tgtEnv, all_exceptions = all_exceptions)
      ## Append to metrics ##
      runTime = datetime.datetime.now() -  splitStartTime
      metricsDF = appendToMetricsDF(metricsDF = metricsDF,
                                          lastLoadDts = "'{}'".format("null"), 
                                          FAILED = True,
                                          PHASE = "'{}'".format("PRE_PROCESS"), 
                                          SOURCE = "'{}'".format(destTable), 
                                          TABLENAME = "'{}'".format(destTable),
                                          FILENAME = "'{}'".format(destTable), 
                                          PROCESSING_TIME = "'{}'".format(runTime), 
                                          RECORDS_READ = "'{}'".format("null"), 
                                          DUP_RECORDS = "'{}'".format("null"), 
                                          QUARANTINED_RECORDS = "'{}'".format("null"), 
                                          EXCEPTIONS = "'{}'".format("null"), 
                                          RECORDS_WRITTEN = "'{}'".format("null"))
  return all_exceptions

# COMMAND ----------

def append_ref_tables_to_snowflake(refSnowflake, referenceDB, snowflakeScheduleOfWork, all_exceptions, tgtEnv, metricsDF):
  '''
  Append needed reference tables to Snowflake tables
  '''
  refSnowflakeStartTime = datetime.datetime.now()
  for refTable in snowflakeScheduleOfWork:
    try:
      if refTable.startswith('TAXO_'):
        refDF = spark.sql("SELECT * FROM {}".format(refTable))
      else:
        refDF = spark.sql("SELECT * FROM {}.{}".format(referenceDB, refTable))

      refDF = refDF.withColumn('LAST_LOAD_DTS', current_timestamp())
      refSnowflake.write(refDF, refTable, 'append')    
      ## Write to metrics table ##
      runTime = datetime.datetime.now() - refSnowflakeStartTime
      metricsDF = appendToMetricsDF(metricsDF = metricsDF,
                                          lastLoadDts = "'{}'".format("null"), 
                                          FAILED = False,
                                          PHASE = "'{}'".format("PRE_PROCESS"), 
                                          SOURCE = "'{}'".format("null"), 
                                          TABLENAME = "'{}'".format(refTable),
                                          FILENAME = "'{}'".format(refTable), 
                                          PROCESSING_TIME = "'{}'".format(runTime), 
                                          RECORDS_READ = "'{}'".format("null"), 
                                          DUP_RECORDS = "'{}'".format("null"), 
                                          QUARANTINED_RECORDS = "'{}'".format("null"), 
                                          EXCEPTIONS = "'{}'".format("null"), 
                                          RECORDS_WRITTEN = "'{}'".format(refDF.count()))
      print('Appended table {} to {}'.format(refTable, referenceDB))       
    except Exception as e:
      print("Failed writing {} to Snowflake. Error message:".format(refTable))
      print(e)
      ## Post to exceptions DF ##
      exceptionVariables = {"FS_POST_ID":"00009", 
                                  "ORIGIN":refTable,
                                  "EXCEPTION_ID_TYPE":"Code Failure", 
                                  "FILE_CREATION_DTS":None, 
                                  "FOR_SUBJECT":None, 
                                  "FOR_BODY":None}
      all_exceptions = generateExceptionDf(exceptionVariables, tgtEnv = tgtEnv, all_exceptions = all_exceptions)
      ## Append to metrics ##
      runTime = datetime.datetime.now() -  refSnowflakeStartTime
      metricsDF = appendToMetricsDF(metricsDF = metricsDF,
                                          lastLoadDts = "'{}'".format("null"), 
                                          FAILED = True,
                                          PHASE = "'{}'".format("PRE_PROCESS"), 
                                          SOURCE = "'{}'".format("null"), 
                                          TABLENAME = "'{}'".format(refTable),
                                          FILENAME = "'{}'".format(refTable), 
                                          PROCESSING_TIME = "'{}'".format(runTime), 
                                          RECORDS_READ = "'{}'".format("null"), 
                                          DUP_RECORDS = "'{}'".format("null"), 
                                          QUARANTINED_RECORDS = "'{}'".format("null"), 
                                          EXCEPTIONS = "'{}'".format("null"), 
                                          RECORDS_WRITTEN = "'{}'".format("null"))
    
  return all_exceptions   

# COMMAND ----------

def generate_fs_id(sourceSeq, padLen = 8):
    """
    * Pads source ID to specified length and base64 encodes to create the 'FS ID'
    * Default pad length is equal to length of EDM IDs
    """
    
    FS_ID = str(sourceSeq).zfill(padLen)
    FS_ID = base64.b64encode(FS_ID.encode())
    FS_ID = FS_ID.decode('ascii')
    
    return FS_ID
  
## This function is used as a UDF within a dataframe, so registering it in Spark here ##  
generate_fs_id_udf = udf(generate_fs_id, StringType())
spark.udf.register("generate_fs_id_udf", generate_fs_id)

# COMMAND ----------

def count_ids_in_bronze_missing_from_ref(bronzeTableName, bronzeTableKey, bronzeDB, refTableName, refTableLookupKey, referenceDB):
    """
    Check for Missing Source IDs in its respective Reference Table. Important for metrics to check MERGE operation results

    Returns: Dictionary of counts
    """
    if '_PIVOT' in bronzeTableName:
      bronzeFullName = bronzeTableName
    else:
      bronzeFullName = '{}.{}'.format(bronzeDB, bronzeTableName)
    
    # Read reference delta table into dataframe
    refTableDF = spark.sql("SELECT * FROM {referenceDB}.{refTableName}".format(referenceDB = referenceDB, refTableName = refTableName))
    # Collect count of reference table
    beforeProcessRefKeys = refTableDF.count()
    
    missingIdsDF = spark.sql("""
                             SELECT DISTINCT {bronzeTableKey} 
                             FROM {bronzeFullName} a 
                             LEFT ANTI JOIN {referenceDB}.{refTableName} b on b.{refTableLookupKey} = a.{bronzeTableKey}
                             WHERE a.{bronzeTableKey} IS NOT NULL
                             """.format(bronzeTableKey = bronzeTableKey,
                                        bronzeFullName = bronzeFullName,
                                        referenceDB = referenceDB,
                                        refTableName = refTableName,
                                        refTableLookupKey = refTableLookupKey)
                            )
    
    # Count of IDs missing from REF
    beforeProcessMissingIds = missingIdsDF.count()
    # Expected count for the end state of REF table
    expectedPostRefKeys = beforeProcessRefKeys + beforeProcessMissingIds
    
    preCountDict = {'refKeysTable': beforeProcessRefKeys, 'sourceIdsMissingFromRef': beforeProcessMissingIds, 'expectedPostRefKeysTable': expectedPostRefKeys}
    
    return preCountDict

# COMMAND ----------

def merge_bronze_into_ref(refMappingDf, tgtEnv, entityName, refDeltaTable, refTablePK, refTableLookupKey, bronzeTableName, bronzeTableKey, bronzeDB, referenceDB):
    """
    Writes a dynamic MERGE statement based off of the smartsheet mapping configuration table
    """
    
    # Filter and transform smartsheet data to get MERGE statement
    entityMappingDf = refMappingDf.filter(col("ENTITY") == entityName)
    
    entityMappingFormatted = entityMappingDf.select(col("*"), concat_ws(".", lit(refDeltaTable), col("refField")).alias("refFieldForm"), concat_ws(".", lit(bronzeTableName), col("bronzeField")).alias("bronzeFieldForm"))

    entityMappingFormatted.createOrReplaceTempView("entityMappingFormatted")
    withFsUDF = spark.sql("""
                          SELECT
                          *,
                          CASE
                            WHEN a.refField = '{authSourcePK}' THEN concat("generate_fs_id_udf(", bronzeFieldForm, ")")
                            WHEN a.bronzeField = 'null' THEN 'NULL'
                            ELSE bronzeFieldForm
                          END as withIdWrap
                          FROM entityMappingFormatted a
                          """.format(authSourcePK = refTablePK))
    
    # Get DDL statement for USING part of MERGE statement
    forUsingCol = withFsUDF.filter(col("refField") != refTablePK).select(col("*"), concat_ws(" AS ", concat(lit("FIRST("), col("withIdWrap"), lit(")")), col("refField")).alias("usingCol"))
      
    usingTableList = [u["usingCol"] for u in forUsingCol.collect()]
      
    seperator = ", "
    usingTableDDL = seperator.join(usingTableList)
    
    ## Different statements for PIVOTED views and actual bronze tables ##
    if '_PIVOT' in bronzeTableName:
      usingSqlStatement = """
                          SELECT 
                          generate_fs_id_udf({table}.{bronzeTableLookupKeyId}) AS {refTablePK},
                          {extraFields}
                          FROM {table} {table}
                          WHERE {table}.{bronzeTableLookupKeyId} IS NOT NULL
                          GROUP BY {table}.{bronzeTableLookupKeyId}, {table}.LastLoadDts
                       """.format(bronzeTableLookupKeyId = bronzeTableKey,
                                  refTablePK = refTablePK,
                                  refTableLookupKey = refTableLookupKey,
                                  table = bronzeTableName,
                                  extraFields = usingTableDDL
                                  )
    else:
      usingSqlStatement = """
                          SELECT 
                          generate_fs_id_udf({table}.{bronzeTableLookupKeyId}) AS {refTablePK},
                          {extraFields}
                          FROM {bronzeDB}.{table} {table}
                          WHERE {table}.{bronzeTableLookupKeyId} IS NOT NULL
                          GROUP BY {table}.{bronzeTableLookupKeyId}, {table}.FileOriginationTimestamp
                          HAVING {table}.FileOriginationTimestamp = (SELECT max(h.FileOriginationTimestamp) FROM {bronzeDB}.{table} h WHERE h.{bronzeTableLookupKeyId} = {table}. {bronzeTableLookupKeyId})
                       """.format(bronzeTableLookupKeyId = bronzeTableKey,
                                  refTablePK = refTablePK,
                                  refTableLookupKey = refTableLookupKey,
                                  bronzeDB = bronzeDB,
                                  table = bronzeTableName,
                                  extraFields = usingTableDDL
                                  )
    
    # Get DDL statement for WHEN MATCHED THEN UPDATE part of MERGE statement
    forWhenList = withFsUDF.collect()
    
    whenMatchedList = [m["refFieldForm"]+" = U."+m["refField"] for m in forWhenList]
    
    seperator = ", "
    whenMatchedDDL = seperator.join(whenMatchedList)
    
    # Get DDL statement for WHEN NOT MATCHED THEN INSERT part of MERGE STATEMENT    
    insertColsFormatted = [c["refFieldForm"] for c in forWhenList]
    insertValsFormatted = ["U."+v["refField"] for v in forWhenList]
    
    seperator = ", "
    seperatedColsList = seperator.join(insertColsFormatted)
    seperatedValsList = seperator.join(insertValsFormatted)
 

    # Create formatted Delta Lake MERGE statement  
    mergeQuery = """
            MERGE INTO {referenceDB}.{targetDeltaTable}
            USING ({usingSqlStatement}) as U
            ON U.{refTablePK} = {targetDeltaTable}.{refTablePK}
            WHEN MATCHED THEN
                UPDATE SET
                  {whenMatched}
            WHEN NOT MATCHED THEN 
                INSERT ({insertColsList})
                VALUES ({insertValsList})
                
            """.format(referenceDB = referenceDB,
                       targetDeltaTable = refDeltaTable, 
                       usingSqlStatement = usingSqlStatement,
                       refTablePK = refTablePK,
                       whenMatched = whenMatchedDDL,
                       insertValsList = seperatedValsList,
                       insertColsList = seperatedColsList,
                       )
    
    # Run Delta Lake MERGE statement
    spark.sql(mergeQuery)

# COMMAND ----------

def find_sequence_and_generate_id(sheetDF):
  """
  - For any REF table with a entity_SEQUENCE and FS_entity_ID column, generates the FS_entity_ID off of the sequence
  - Expects a Spark dataframe as input
  - Dependent on FsIdGenerator being checked in as a UDF named generate_fs_id_udf
  Returns:
  * A modified dataframe with the FS ID value inserted
  """

  colNames = sheetDF.columns
  
  seqPattern = re.compile(".+_SEQUENCE$")
  seqColList = list(filter(seqPattern.match, colNames))
  seqCol = seqColList[0]
  
  idPattern = re.compile("^FS_.+_ID$")
  idColList = list(filter(idPattern.match, colNames))
  idCol = idColList[0]
  
  dropDF = sheetDF.drop(col(idCol))
  
  fullDF = dropDF.withColumn(idCol, generate_fs_id_udf(col(seqCol)))
  
  return fullDF

# COMMAND ----------

def collect_column_ids_of_smartsheet(sheetId, accessToken):
    """
    Returns the column names and Ids in a dictionary of a given smartsheet
    """

    requestHeaders = {"Authorization":"Bearer {}".format(accessToken), 
                        "Content-Type":"application/json"}

    ## Get data from current smartsheet ##
    jsonResponse = requests.get("https://api.smartsheet.com/2.0/sheets/{}".format(sheetId), headers = requestHeaders).json()

    ## Get column names and IDs ##
    colDict = {x["title"]: x["id"] for x in jsonResponse["columns"]} 

    return colDict

# COMMAND ----------

def generateIdsAndUpdateSmartsheet(df, sheetId, smartSheetAccessToken):
    """
    Generate missing IDs from sequence column, and write new IDs back to Smartsheet grid
    """

    ## Find the Id column of the table ##
    colNames = df.columns
    idPattern = re.compile("^FS_.+_ID$")
    idColList = list(filter(idPattern.match, colNames))
    ## Makes sure to only process if table has an Id column. (Country ref table does not have Ids, for example) ##
    statusMessage = "No IDs missing. Smartsheet not updated. (intentional)"
    if len(idColList) != 0:
        idCol = idColList[0]
        nullSheetIds = df.filter(col(idCol).isNull())
        ## If there are nullIds in the table, generate IDs from the sequence number ##
        if len(nullSheetIds.collect()) != 0:
            sheetWithIdsGenerated = find_sequence_and_generate_id(nullSheetIds)

            ## Write back updated ID values to smartsheet ##
            columnDict = collect_column_ids_of_smartsheet(sheetId, smartSheetAccessToken)

            sheetWithIdsList = sheetWithIdsGenerated.collect()

            bulkCells = [{"id": row["id"], "cells": [{"value": row[idCol], "columnId": columnDict[idCol]}]} for row in sheetWithIdsList]

            bulkPost = json.dumps(bulkCells)

            postAttempt = requests.put('https://api.smartsheet.com/2.0/sheets/'+sheetId+'/rows', data = bulkPost, headers = {'Authorization':'Bearer '+smartSheetAccessToken})
            statusMessage = json.loads(postAttempt.text)
            statusMessage = statusMessage["message"]

        else:
            pass
    else:
        pass
    print("{}".format(statusMessage))

# COMMAND ----------

def updateDuctTapeView(referenceDB):
  """
  Truncates and rebuilds TMP_SLEEVES_DUCT_TAPE_ALL
  
  This is a temporary solution
  """
  ## Truncate ##
  spark.sql("truncate table {referenceDB}.TMP_SLEEVES_DUCT_TAPE_ALL".format(referenceDB = referenceDB))
  ## Re-Populate View with New Data ##
  ductTapeData = spark.sql("""select  FS_SLEEVE_ID,
                    FS_FUND_ID,
                    FS_SLEEVE_SHORT_ID,
                    FS_SLEEVE_SHORT_NAME,
                    FS_SLEEVE_LONG_NAME,
                    STATESTREET_SLEEVE_ID,
                    GENEVA_SLEEVE_ID,
                    POMA_SLEEVE_ID,
                    KKR_TRADES_SLEEVE_ID,
                    BMS_SLEEVE_ID,
                    ARTEMIS_SLEEVE_ID,
                    VIRTUS_SLEEVE_ID,
                    JPM_SLEEVE_ID,
                    BNP_SLEEVE_ID,
                    WELLS_FARGO_SLEEVE_ID,
                    FLAG_DELETE,
                    EFFECTIVE_DTS,
                    LAST_LOAD_DTS,
                    SOURCE_META_DTS,
                    REC_SRC_FEED,
                    REC_PREFIX,
                    REC_SRC,
                    REC_SIGNATURE
              from {referenceDB}.REF_FUND_SLEEVE_KEYS

              UNION ALL

              select sleeve.FS_SLEEVE_ID,
                  sleeve.FS_FUND_ID,
                  sleeve.FS_SLEEVE_SHORT_ID,
                  sleeve.FS_SLEEVE_SHORT_NAME,
                  sleeve.FS_SLEEVE_LONG_NAME,
                  duct.STATESTREET_ID as STATESTREET_SLEEVE_ID,
                  duct.GENEVA_ID AS GENEVA_SLEEVE_ID,
                  duct.BIMS_ID as POMA_SLEEVE_ID,
                  duct.KKR_ID as KKR_TRADES_SLEEVE_ID,
                  duct.BMS_ID as BMS_SLEEVE_ID,
                  duct.ARTEMIS_ID as ARTEMIS_SLEEVE_ID,
                  duct.VIRTUS_ID AS VIRTUS_SLEEVE_ID,
                  duct.JPM_ID AS JPM_SLEEVE_ID,
                  duct.BNP_ID AS BNP_SLEEVE_ID,
                  duct.WELLS_FARGO_ID as WELLS_FARGO_SLEEVE_ID,
                  duct.FLAG_DELETE,
                  sleeve.LAST_LOAD_DTS,
                  sleeve.REC_SRC,
                  sleeve.EFFECTIVE_DTS,
                  sleeve.SOURCE_META_DTS,
                  sleeve.REC_SRC_FEED,
                  sleeve.REC_PREFIX,
                  sleeve.REC_SIGNATURE
              from {referenceDB}.REF_TMP_SLEEVE_DUCT_TAPE duct
              inner join {referenceDB}.REF_FUND_SLEEVE_KEYS sleeve on sleeve.FS_SLEEVE_ID = duct.FS_SLEEVE_ID
              where duct.DUCT_TAPE_TYPE = 'SLEEVE'

              order by FS_SLEEVE_ID
              """.format(referenceDB = referenceDB))
  ductTapeSchema = mainSchemaFunction(dbName = referenceDB, tblName = 'TMP_SLEEVES_DUCT_TAPE_ALL', df = ductTapeData, enforceSchema = True)
  ductTapeSchema.df.write.format('delta').mode('overwrite').saveAsTable('{}.TMP_SLEEVES_DUCT_TAPE_ALL'.format(referenceDB))


# COMMAND ----------

def update_ref_rating_concat_table(referenceDB):
  """
  Truncates and rebuilds CONCATENATED_REF_RATINGS_CODES
  This is a temporary solution
  """
  ## Re-Populate View with New Data ##
  ref_rating_concat = spark.sql("""
    /* MOODYS */
    select distinct FS_RATING_ID, CONCAT_WS('|', UPPER(MOODYS_RATING), 'MOODYS', UPPER(RATING_TYPE)) AS REF_CONCAT
    from {referenceDB}.REF_RATINGS_CODES
    where MOODYS_RATING IS NOT NULL

    UNION ALL

    /* SP */
    select distinct FS_RATING_ID, CONCAT_WS('|', UPPER(SP_RATING), 'SP', UPPER(RATING_TYPE)) AS REF_CONCAT
    from {referenceDB}.REF_RATINGS_CODES
    where SP_RATING IS NOT NULL

    UNION ALL

    /* FITCH */
    select distinct FS_RATING_ID, CONCAT_WS('|', UPPER(FITCH_RATING), 'FITCH', UPPER(RATING_TYPE)) AS REF_CONCAT
    from {referenceDB}.REF_RATINGS_CODES
    where FITCH_RATING IS NOT NULL
     """.format(referenceDB = referenceDB))
  ref_rating_concat.write.format('delta').mode('overwrite').saveAsTable('{}.CONCATENATED_REF_RATINGS_CODES'.format(referenceDB))

# COMMAND ----------

def update_ref_industry_concat_table(referenceDB):
  """
  Truncates and rebuilds CONCATENATED_REF_INDUSTRY_CODES
  This is a temporary solution
  """
  ## Re-Populate View with New Data ##
  ref_industry_concat = spark.sql("""
        /* GICS_SECTOR */
      select distinct FS_INDUSTRY_ID, CONCAT_WS('|', UPPER(GICS_SECTOR_NAME), 'GICS_SECTOR_NAME') AS INDUSTRY_CONCAT
      from {referenceDB}.REF_INDUSTRY_CODES
      where GICS_SECTOR_NAME IS NOT NULL
      and GICS_INDUSTRY_GROUP_NAME IS NULL
      and GICS_INDUSTRY_NAME IS NULL
      and GICS_SUB_INDUSTRY_NAME IS NULL

      UNION ALL

      /* GICS_INDUSTRY_GROUP */ 
      select distinct FS_INDUSTRY_ID, CONCAT_WS('|', UPPER(GICS_INDUSTRY_GROUP_NAME), 'GICS_INDUSTRY_GROUP_NAME') AS INDUSTRY_CONCAT
      from {referenceDB}.REF_INDUSTRY_CODES
      where GICS_INDUSTRY_GROUP_NAME IS NOT NULL
      and GICS_INDUSTRY_NAME IS NULL
      and GICS_SUB_INDUSTRY_NAME IS NULL

      UNION ALL

      /* GICS_INDUSTRY */
      select distinct FS_INDUSTRY_ID, CONCAT_WS('|', UPPER(GICS_INDUSTRY_NAME), 'GICS_INDUSTRY_NAME') AS INDUSTRY_CONCAT
      from {referenceDB}.REF_INDUSTRY_CODES
      where GICS_INDUSTRY_NAME IS NOT NULL
      and GICS_SUB_INDUSTRY_NAME IS NULL

      UNION ALL

      /* GICS_SUB_INDUSTRY */
      select distinct FS_INDUSTRY_ID, CONCAT_WS('|', UPPER(GICS_SUB_INDUSTRY_NAME), 'GICS_SUB_INDUSTRY_NAME') AS INDUSTRY_CONCAT
      from {referenceDB}.REF_INDUSTRY_CODES
      where GICS_SUB_INDUSTRY_NAME IS NOT NULL

      UNION ALL

      /* ICB_INDUSTRY */
      select distinct FS_INDUSTRY_ID, CONCAT_WS('|', UPPER(ICB_INDUSTRY_NAME), 'ICB_INDUSTRY_NAME') AS INDUSTRY_CONCAT
      from {referenceDB}.REF_INDUSTRY_CODES
      where ICB_INDUSTRY_NAME IS NOT NULL
      and ICB_SUPER_SECTOR_NAME IS NULL
      and ICB_SECTOR_NAME IS NULL
      and ICB_SUB_SECTOR_NAME IS NULL

      UNION ALL

      /* ICB_SUPER_SECTOR */
      select distinct FS_INDUSTRY_ID, CONCAT_WS('|', UPPER(ICB_SUPER_SECTOR_NAME), 'ICB_SUPER_SECTOR_NAME') AS INDUSTRY_CONCAT
      from odin_reference.ref_industry_codes
      where ICB_SUPER_SECTOR_NAME IS NOT NULL
      and ICB_SECTOR_NAME IS NULL
      and ICB_SUB_SECTOR_NAME IS NULL

      UNION ALL

      /* ICB_SECTOR */
      select distinct FS_INDUSTRY_ID, CONCAT_WS('|', UPPER(ICB_SECTOR_NAME), 'ICB_SECTOR_NAME') AS INDUSTRY_CONCAT
      from {referenceDB}.REF_INDUSTRY_CODES
      where ICB_SECTOR_NAME IS NOT NULL
      and ICB_SUB_SECTOR_NAME IS NULL

      UNION ALL

      /* ICB_SUB_SECTOR */
      select distinct FS_INDUSTRY_ID, CONCAT_WS('|', UPPER(ICB_SUB_SECTOR_NAME), 'ICB_SUB_SECTOR_NAME') AS INDUSTRY_CONCAT
      from {referenceDB}.REF_INDUSTRY_CODES
      where ICB_SUB_SECTOR_NAME IS NOT NULL

      UNION ALL 

      /* MOODYS_35 */
      select distinct FS_INDUSTRY_ID, CONCAT_WS('|', UPPER(TEMP_MOODY_35_NAME), 'TEMP_MOODY_35_NAME') AS INDUSTRY_CONCAT
      from {referenceDB}.REF_INDUSTRY_CODES
      where TEMP_MOODY_35_NAME IS NOT NULL

      UNION ALL

      /* FSEP */
      select distinct FS_INDUSTRY_ID, CONCAT_WS('|', UPPER(TEMP_FSEP_NAME), 'TEMP_FSEP_NAME') AS INDUSTRY_CONCAT
      from {referenceDB}.REF_INDUSTRY_CODES
      where TEMP_FSEP_NAME IS NOT NULL
       """.format(referenceDB = referenceDB))
  ref_industry_concat.write.format('delta').mode('overwrite').saveAsTable('{}.CONCATENATED_REF_INDUSTRY_CODES'.format(referenceDB))

# COMMAND ----------

def updateHubTable(referenceDB, silverDB, silverSnowflake, tgtEnv, all_exceptions, metricsDF):
  """
  Updates HUB_entity table based off of reference table
  """
  startTime = datetime.datetime.now()
  
  silverHubConfig = preProcessorLocal["config-preprocessor-hub-tables_DF"].collect()
  ## For each file in configs##
  for hub in silverHubConfig:
    try:
      entity = hub["entity"]
      refTable = hub["refTable"]
      destTable = 'HUB_{}'.format(entity)
      hubDf = spark.sql("""
                        SELECT FS_{entity}_ID AS HK_{entity}, 
                              FS_{entity}_ID,
                              FLAG_DELETE, 
                              EFFECTIVE_DTS AS START_DTS,
                              current_timestamp() AS LAST_LOAD_DTS,
                              REC_SRC,
                              REC_PREFIX
                        FROM {refDB}.{refTable}
                        """.format(entity = entity, refDB = referenceDB, refTable = refTable))

      silverSnowflake.write(hubDf, destTable, 'append')
      ## Write to metrics table ##
      runTime = datetime.datetime.now() -  startTime
      metricsDF = appendToMetricsDF(metricsDF = metricsDF,
                                          lastLoadDts = "'{}'".format("null"), 
                                          FAILED = False,
                                          PHASE = "'{}'".format("PRE_PROCESS"), 
                                          SOURCE = "'{}'".format(refTable), 
                                          TABLENAME = "'{}'".format(refTable),
                                          FILENAME = "'{}'".format("null"), 
                                          PROCESSING_TIME = "'{}'".format(runTime), 
                                          RECORDS_READ = "'{}'".format("null"), 
                                          DUP_RECORDS = "'{}'".format("null"), 
                                          QUARANTINED_RECORDS = "'{}'".format("null"), 
                                          EXCEPTIONS = "'{}'".format("null"), 
                                          RECORDS_WRITTEN = "'{}'".format(hubDf.count()))
      print("Updated {}.HUB_{}".format(referenceDB, entity))
    except:
      print("Failed to update HUB_{}".format(entity))
      ## Post to exceptions DF ##
      exceptionVariables = {"FS_POST_ID":"00009", 
                                  "ORIGIN":refTable,
                                  "EXCEPTION_ID_TYPE":"Code Failure", 
                                  "FILE_CREATION_DTS":None, 
                                  "FOR_SUBJECT":None, 
                                  "FOR_BODY":None}
      all_exceptions = generateExceptionDf(exceptionVariables, tgtEnv = tgtEnv, all_exceptions = all_exceptions)
      ## Append to metrics ##
      runTime = datetime.datetime.now() -  startTime
      metricsDF = appendToMetricsDF(metricsDF = metricsDF,
                                          lastLoadDts = "'{}'".format("null"), 
                                          FAILED = True,
                                          PHASE = "'{}'".format("PRE_PROCESS"), 
                                          SOURCE = "'{}'".format(refTable), 
                                          TABLENAME = "'{}'".format(refTable),
                                          FILENAME = "'{}'".format("null"), 
                                          PROCESSING_TIME = "'{}'".format(runTime), 
                                          RECORDS_READ = "'{}'".format("null"), 
                                          DUP_RECORDS = "'{}'".format("null"), 
                                          QUARANTINED_RECORDS = "'{}'".format("null"), 
                                          EXCEPTIONS = "'{}'".format("null"), 
                                          RECORDS_WRITTEN = "'{}'".format("null"))
  return all_exceptions    

# COMMAND ----------

def updateAutomaticallyMaintainedRefTables(preProcessorLocal, bronzeDB, referenceDB, tgtEnv, all_exceptions, metricsDF):
  """
  * Update reference tables based off of configuration Smartsheets
  * Relies on the following two Smartsheets to operate:
    - REFERENCE_PREPROCESSOR_CONFIG
    - REFERENCE_PREPROCESSOR_MAPPING
  """
  startTime = datetime.datetime.now()
  ## Create lists from config and mapping sheets ##
  selfMaintainedConfigs = preProcessorLocal["config-preprocessor-link-ref_DF"].collect()
  selfMaintainedMappingDF = preProcessorLocal["map-preprocessor-link-ref_DF"]
  
  for entity in selfMaintainedConfigs:
    refDeltaTable = entity["refDeltaTable"]
    refTableLookupKey = entity["refTableLookupKey"]
    refTablePK = entity["refTablePK"]
    bronzeTableName = entity["bronzeDeltaTable"]
    bronzeTableKey = entity["bronzeTableLookupKey"]
    entityName = entity["ENTITY"]  
    try:
      ## Get counts of reference table and missing IDs before merge starts ##
      preMissingIds = count_ids_in_bronze_missing_from_ref(bronzeTableName, bronzeTableKey, bronzeDB, refDeltaTable, refTableLookupKey, referenceDB)
      print("REF_{}_KEYS count at start: {}".format(entityName, preMissingIds["refKeysTable"]))
      print("{} IDs in bronze table missing from reference table".format(preMissingIds["sourceIdsMissingFromRef"]))
      print("Expected end count: {}".format(preMissingIds['expectedPostRefKeysTable']))

      ## Write dynamic SQL statement and MERGE data into Delta reference table ##
      merge_bronze_into_ref(selfMaintainedMappingDF, tgtEnv, entityName, refDeltaTable, refTablePK, refTableLookupKey, bronzeTableName, bronzeTableKey, bronzeDB, referenceDB)
      ## Add table to the schedule of work for snowflake writes ##
      snowflakeScheduleOfWork.append(refDeltaTable)
      ## Count IDs after merge to make sure it matches expected ##
      postMissingIds = spark.sql("SELECT * FROM {referenceDB}.{refTableName}".format(referenceDB = referenceDB, refTableName = refDeltaTable)).count()
      print("REF_{}_KEYS after merge: {}".format(entityName, postMissingIds))
      ## Write to metrics table ##
      runTime = datetime.datetime.now() - startTime
      metricsDF = appendToMetricsDF(metricsDF = metricsDF,
                                          lastLoadDts = "'{}'".format("null"), 
                                          FAILED = False,
                                          PHASE = "'{}'".format("PRE_PROCESS"), 
                                          SOURCE = "'{}'".format(bronzeTableName), 
                                          TABLENAME = "'{}'".format(refDeltaTable),
                                          FILENAME = "'{}'".format("null"), 
                                          PROCESSING_TIME = "'{}'".format(runTime), 
                                          RECORDS_READ = "'{}'".format("null"), 
                                          DUP_RECORDS = "'{}'".format("null"), 
                                          QUARANTINED_RECORDS = "'{}'".format("null"), 
                                          EXCEPTIONS = "'{}'".format("null"), 
                                          RECORDS_WRITTEN = "'{}'".format(preMissingIds["sourceIdsMissingFromRef"]))
    except Exception as e:
      print("Failed to update {}".format(refDeltaTable))
      print(e)
      exceptionVariables = {"FS_POST_ID":"00009", 
                            "ORIGIN":refDeltaTable,
                            "EXCEPTION_ID_TYPE":"Code Failure", 
                            "FILE_CREATION_DTS":None, 
                            "FOR_SUBJECT":None, 
                            "FOR_BODY":None}
      all_exceptions = generateExceptionDf(exceptionVariables, tgtEnv = tgtEnv, all_exceptions = all_exceptions)
      ## Append to metrics ##
      runTime = datetime.datetime.now() - startTime
      metricsDF = appendToMetricsDF(metricsDF = metricsDF,
                                          lastLoadDts = "'{}'".format("null"), 
                                          FAILED = True,
                                          PHASE = "'{}'".format("PRE_PROCESS"), 
                                          SOURCE = "'{}'".format(bronzeTableName), 
                                          TABLENAME = "'{}'".format(refDeltaTable),
                                          FILENAME = "'{}'".format("null"), 
                                          PROCESSING_TIME = "'{}'".format(runTime), 
                                          RECORDS_READ = "'{}'".format("null"), 
                                          DUP_RECORDS = "'{}'".format("null"), 
                                          QUARANTINED_RECORDS = "'{}'".format("null"), 
                                          EXCEPTIONS = "'{}'".format("null"), 
                                          RECORDS_WRITTEN = "'{}'".format("null"))
    
  return all_exceptions

# COMMAND ----------

def updateSmartSheetMaintainedRefTables(preProcessorLocal, refSheetIds, referenceDB, silverDB, jobDB, tgtEnv, all_exceptions, metricsDF):
    """
    * Generates FS_IDs where missing
    * Overwrites DELTA reference tables based off of reference smartsheets
    * Smartsheets are stored in the ODIN_REFERENCE workspace 
    """
    startTime = datetime.datetime.now()
    ## Subset reference dictionary to the tables that we want ##
    ## Necessary to do this to exclude the config sheets from the dictionary ##
    ## Change back to below line if we end up running TAXOs through this same process rather than new function ##
    smartsheetMaintainedTables = {x: y for x, y in preProcessorLocal.items() if x.startswith('REF_')}
    ## Set this here because it was not getting passed through to the function if declared elsewhere ##
    spark.conf.set("spark.sql.execution.arrow.enabled", "false")
    try:
        ## Create hashes for each dataframe ##
        refHashes = {}
        print("Creating hash table of current Smartsheets")
        for x in smartsheetMaintainedTables.keys():
            sheetPdDf = preProcessorLocal[x].toPandas()
            ## Generate a hash of each line of the pandas dataframe ##
            h0 = hash_pandas_object(sheetPdDf).values
            ## Generate SHA256 hash of all the rows (the whole DF) ##
            h = hashlib.sha256(h0).hexdigest()
            refHashes[x] = h

        ## Now that we have refHashes dictionary, convert to a spark dataframe ##
        ## Reformat dictionary to list like [(key1, value1), (key2, value2)] ##
        listOfHashes = [(k, v) for k, v in refHashes.items()]

        formattedData = sc.parallelize(listOfHashes)

        # Describe schema for Spark DF
        currentSchema = StructType ([
                                    StructField("smartsheetName", StringType(), True),
                                    StructField("hashKey", StringType(), True)
                                ])
        # Create hashDF
        currentSheetsDF = spark.createDataFrame(formattedData, currentSchema)
        currentSheetsDF.createOrReplaceTempView("VW_currentSheetsDF")
        existingSheetsDF = spark.sql("SELECT * FROM {jobDB}.ref_hash_key_map WHERE smartsheetName LIKE 'REF_%'".format(jobDB = jobDB))
        # Replace above line with below line if TAXOs end up going through same process as REF
        existingSheetsDF.createOrReplaceTempView("VW_existingSheetsDF")
        print("Comparing current hash table to existing hash table")
        differencesDF = spark.sql(
                                """
                                SELECT c.*
                                FROM VW_currentSheetsDF c
                                LEFT ANTI JOIN VW_existingSheetsDF e ON c.hashkey = e.hashkey
                                """)

        tablesToUpdate = differencesDF.select(col("smartsheetName")).collect()
        tablesDifferentForMetrics = len(tablesToUpdate)
        if len(tablesToUpdate) > 0:
          print("The following tables have changed and need to be updated:")
          for sheet in tablesToUpdate:
              print("{}".format(sheet["smartsheetName"]))
        else:
            print("No tables have changed since last run")
        ## Process each reference table for missing IDs and schema changes ##
        for table in tablesToUpdate:
          tableName = table["smartsheetName"]
          deltaName = tableName.rstrip("_DF")
          tableDF = preProcessorLocal[tableName]
          sheetId = refSheetIds[tableName]
          print("Processing: {}".format(tableName))
          print("Running: generateIdsAndUpdateSmartsheet for {}".format(tableName))
          generateIdsAndUpdateSmartsheet(tableDF, sheetId, smart_sheet_access_token)
          ## Update dictionary with new dataframe, after IDs were sent back ##
          preProcessorLocal[tableName] = getSmartSheetDf(smart_sheet_access_token, sheetId)
          updatedTableDF = preProcessorLocal[tableName]
          updatedTableDF = updatedTableDF.drop('id', 'rowNumber', 'parentId')
          ## Remove sequence col if present ##
          colNames = updatedTableDF.columns
          seqPattern = re.compile(".+_SEQUENCE$")
          seqColList = list(filter(seqPattern.match, colNames))
          if len(seqColList) != 0:
            seqCol = seqColList[0]
            updatedTableDF = updatedTableDF.drop(seqCol)
          else:
            pass
          formattedSchema = mainSchemaFunction(dbName = referenceDB, tblName = deltaName, df = updatedTableDF, enforceSchema = True)
          castedDF = formattedSchema.df
          ## Update LAST_LOAD_DTS ##
          finalDF = castedDF.withColumn('LAST_LOAD_DTS', current_timestamp())
          ## Overwrite Delta table ##
          finalDF.write.format("delta").mode("overwrite").saveAsTable("{referenceDB}.{refTable}".format(referenceDB = referenceDB, refTable = deltaName))
          print('Wrote {} to Delta'.format(deltaName))
          snowflakeScheduleOfWork.append(deltaName)

        ## Overwrite stored hash table at end of process ##
        ## Create hashes for each dataframe ##
        finalRefHashes = {}
        for x in smartsheetMaintainedTables.keys():
            sheetPdDf = preProcessorLocal[x].toPandas()
            ## Generate a hash of each line of the pandas dataframe ##
            h0 = hash_pandas_object(sheetPdDf).values
            ## Generate SHA256 hash of all the rows (the whole DF) ##
            h = hashlib.sha256(h0).hexdigest()
            finalRefHashes[x] = h

        ## Now that we have refHashes dictionary, convert to a spark dataframe ##
        ## Reformat dictionary to list like [(key1, value1), (key2, value2)] ##
        finalListOfHashes = [(k, v) for k, v in finalRefHashes.items()]

        finalFormattedData = sc.parallelize(finalListOfHashes)

        ## Describe schema for Spark DF ##
        finalSchema = StructType ([
                                    StructField("smartsheetName", StringType(), True),
                                    StructField("hashKey", StringType(), True)
                                    ])
        ## Create hashDF ##
        finalSheetsDF = spark.createDataFrame(finalFormattedData, finalSchema)
        finalSheetsDF.createOrReplaceTempView('ref_final_sheet_hashes')
        spark.sql(""" MERGE INTO {}.ref_hash_key_map d
                      USING ref_final_sheet_hashes i
                      ON i.smartsheetName = d.smartsheetName
                      WHEN MATCHED THEN UPDATE SET *
                      WHEN NOT MATCHED THEN INSERT *
                  """.format(jobDB))
        print("Updated hash table at end-of-process")

        ## Update dependencies on Smartsheet maintained tables ##
        for table in tablesToUpdate:
          tableName = table["smartsheetName"]
          if tableName == "REF_TMP_SLEEVE_DUCT_TAPE_DF" or tableName == "REF_FUND_SLEEVE_KEYS_DF":
            try:
              updateDuctTapeView(referenceDB)
              snowflakeScheduleOfWork.append('TMP_SLEEVES_DUCT_TAPE_ALL')                            
              print("Updated TMP_SLEEVES_DUCT_TAPE_ALL")
            except:
              print("Failed to run updateDuctTapeView")
              ## Post to exceptions DF ##
              exceptionVariables = {"FS_POST_ID":"00009", 
                                          "ORIGIN":"TMP_SLEEVES_DUCT_TAPE_ALL",
                                          "EXCEPTION_ID_TYPE":"Code Failure", 
                                          "FILE_CREATION_DTS":None, 
                                          "FOR_SUBJECT":None, 
                                          "FOR_BODY":None}
              all_exceptions = generateExceptionDf(exceptionVariables, tgtEnv = tgtEnv, all_exceptions = all_exceptions)
              ## Append to metrics ##
              runTime = datetime.datetime.now() - startTime
              metricsDF = appendToMetricsDF(metricsDF = metricsDF,
                                                  lastLoadDts = "'{}'".format("null"), 
                                                  FAILED = True,
                                                  PHASE = "'{}'".format("PRE_PROCESS"), 
                                                  SOURCE = "'{}'".format("TMP_SLEEVES_DUCT_TAPE_ALp"), 
                                                  TABLENAME = "'{}'".format("TMP_SLEEVES_DUCT_TAPE_AL"),
                                                  FILENAME = "'{}'".format("null"), 
                                                  PROCESSING_TIME = "'{}'".format(runTime), 
                                                  RECORDS_READ = "'{}'".format("null"), 
                                                  DUP_RECORDS = "'{}'".format("null"), 
                                                  QUARANTINED_RECORDS = "'{}'".format("null"), 
                                                  EXCEPTIONS = "'{}'".format("null"), 
                                                  RECORDS_WRITTEN = "'{}'".format("null"))
         
          else:
            pass
    
    except Exception as e:
      print(e)
      print("Failed to run updateSmartSheetMaintainedRefTables")
      ## Post to exceptions DF ##
      exceptionVariables = {"FS_POST_ID":"00009", 
                                  "ORIGIN":"refHashMap",
                                  "EXCEPTION_ID_TYPE":"Code Failure", 
                                  "FILE_CREATION_DTS":None, 
                                  "FOR_SUBJECT":None, 
                                  "FOR_BODY":None}
      all_exceptions = generateExceptionDf(exceptionVariables, tgtEnv = tgtEnv, all_exceptions = all_exceptions)
      ## Append to metrics ##
      runTime = datetime.datetime.now() - startTime
      metricsDF = appendToMetricsDF(metricsDF = metricsDF,
                                          lastLoadDts = "'{}'".format("null"), 
                                          FAILED = True,
                                          PHASE = "'{}'".format("PRE_PROCESS"), 
                                          SOURCE = "'{}'".format("refHashMap"), 
                                          TABLENAME = "'{}'".format("null"),
                                          FILENAME = "'{}'".format("null"), 
                                          PROCESSING_TIME = "'{}'".format(runTime), 
                                          RECORDS_READ = "'{}'".format("null"), 
                                          DUP_RECORDS = "'{}'".format("null"), 
                                          QUARANTINED_RECORDS = "'{}'".format("null"), 
                                          EXCEPTIONS = "'{}'".format("null"), 
                                          RECORDS_WRITTEN = "'{}'".format("null"))
    return all_exceptions

# COMMAND ----------

def updateSmartSheetMaintainedTAXOTables(preProcessorLocal, refSheetIds, silverDB, jobDB, tgtEnv, all_exceptions, metricsDF):
    """
    * Smartsheets are stored in the ODIN_REFERENCE workspace 
    """
    startTime = datetime.datetime.now()
    ## Subset reference dictionary to the tables that we want ##
    ## Necessary to do this to exclude the config and ref sheets from the dictionary ##
    taxoSmartsheetTables = {x: y for x, y in preProcessorLocal.items() if x.startswith('TAXO_')}
    ## Set this here because it was not getting passed through to the function if declared elsewhere ##
    spark.conf.set("spark.sql.execution.arrow.enabled", "false")
    try:
        ## Create hashes for each dataframe ##
        taxoHashes = {}
        print("Creating hash table of current Smartsheets")
        for x in taxoSmartsheetTables.keys():
            sheetPdDf = preProcessorLocal[x].toPandas()
            ## Generate a hash of each line of the pandas dataframe ##
            h0 = hash_pandas_object(sheetPdDf).values
            ## Generate SHA256 hash of all the rows (the whole DF) ##
            h = hashlib.sha256(h0).hexdigest()
            taxoHashes[x] = h

        ## Now that we have refHashes dictionary, convert to a spark dataframe ##
        ## Reformat dictionary to list like [(key1, value1), (key2, value2)] ##
        listOfHashes = [(k, v) for k, v in taxoHashes.items()]

        formattedData = sc.parallelize(listOfHashes)

        # Describe schema for Spark DF
        currentSchema = StructType ([
                                    StructField("smartsheetName", StringType(), True),
                                    StructField("hashKey", StringType(), True)
                                ])
        # Create hashDF
        currentSheetsDF = spark.createDataFrame(formattedData, currentSchema)
        currentSheetsDF.createOrReplaceTempView("taxo_current_hashes")

        existingSheetsDF = spark.sql("SELECT * FROM {}.ref_hash_key_map WHERE smartsheetName LIKE 'TAXO_%'".format(jobDB))
        existingSheetsDF.createOrReplaceTempView("taxo_existing_hashes")
        print("Comparing current hash table to existing hash table")
        differencesDF = spark.sql(
                                """
                                SELECT c.*
                                FROM taxo_current_hashes c
                                LEFT ANTI JOIN taxo_existing_hashes e ON c.hashkey = e.hashkey
                                """)

        tablesToUpdate = differencesDF.select(col("smartsheetName")).collect()
        tablesDifferentForMetrics = len(tablesToUpdate)
        if len(tablesToUpdate) > 0:
          print("The following tables have changed and need to be updated:")
          for sheet in tablesToUpdate:
              print("{}".format(sheet["smartsheetName"]))
        else:
            print("No tables have changed since last run")
        ## Process each reference table for missing IDs and schema changes ##
        for table in tablesToUpdate:
          tableName = table["smartsheetName"]
          deltaName = tableName.rstrip("_DF")
          tableDF = preProcessorLocal[tableName]
          sheetId = refSheetIds[tableName]
          print("Processing: {}".format(tableName))
          print("Running: generateIdsAndUpdateSmartsheet for {}".format(tableName))
          generateIdsAndUpdateSmartsheet(tableDF, sheetId, smart_sheet_access_token)
          ## Update dictionary with new dataframe, after IDs were sent back ##
          preProcessorLocal[tableName] = getSmartSheetDf(smart_sheet_access_token, sheetId)
          updatedTableDF = preProcessorLocal[tableName]
          updatedTableDF = updatedTableDF.drop('id', 'rowNumber', 'parentId')
          ## Remove sequence col if present ##
          colNames = updatedTableDF.columns
          seqPattern = re.compile(".+_SEQUENCE$")
          seqColList = list(filter(seqPattern.match, colNames))
          if len(seqColList) != 0:
            seqCol = seqColList[0]
            updatedTableDF = updatedTableDF.drop(seqCol)
          else:
            pass
          ## Update LAST_LOAD_DTS ##
          finalDF = updatedTableDF.withColumn('LAST_LOAD_DTS', current_timestamp())
          finalDF = finalDF.withColumn('EFFECTIVE_DTS', to_timestamp(col('EFFECTIVE_DTS')))
          finalDF = finalDF.withColumn('SOURCE_META_DTS', to_timestamp(col('SOURCE_META_DTS')))
          finalDF.createOrReplaceTempView(deltaName)
          snowflakeScheduleOfWork.append(deltaName)

        ## Overwrite stored hash table at end of process ##
        ## Create hashes for each dataframe ##
        finalTaxoHashes = {}
        for x in taxoSmartsheetTables.keys():
            sheetPdDf = preProcessorLocal[x].toPandas()
            ## Generate a hash of each line of the pandas dataframe ##
            h0 = hash_pandas_object(sheetPdDf).values
            ## Generate SHA256 hash of all the rows (the whole DF) ##
            h = hashlib.sha256(h0).hexdigest()
            finalTaxoHashes[x] = h

        ## Now that we have refHashes dictionary, convert to a spark dataframe ##
        ## Reformat dictionary to list like [(key1, value1), (key2, value2)] ##
        finalListOfHashes = [(k, v) for k, v in finalTaxoHashes.items()]

        finalFormattedData = sc.parallelize(finalListOfHashes)

        ## Describe schema for Spark DF ##
        finalSchema = StructType ([
                                    StructField("smartsheetName", StringType(), True),
                                    StructField("hashKey", StringType(), True)
                                    ])
        ## Create hashDF ##
        finalSheetsDF = spark.createDataFrame(finalFormattedData, finalSchema)
        finalSheetsDF.createOrReplaceTempView('taxo_final_hashes')
        spark.sql(""" MERGE INTO {}.ref_hash_key_map d
                      USING taxo_final_hashes i
                      ON i.smartsheetName = d.smartsheetName
                      WHEN MATCHED THEN UPDATE SET *
                      WHEN NOT MATCHED THEN INSERT *
                  """.format(jobDB))
        print("Updated hash table at end-of-process")
    
    except Exception as e:
      print(e)
      print("Failed to run updateSmartSheetMaintainedTAXOTables")
      ## Post to exceptions DF ##
      exceptionVariables = {"FS_POST_ID":"00009", 
                                  "ORIGIN":"updateSmartSheetMaintainedTAXOTables",
                                  "EXCEPTION_ID_TYPE":"Code Failure", 
                                  "FILE_CREATION_DTS":None, 
                                  "FOR_SUBJECT":None, 
                                  "FOR_BODY":None}
      all_exceptions = generateExceptionDf(exceptionVariables, tgtEnv = tgtEnv, all_exceptions = all_exceptions)
      ## Append to metrics ##
      runTime = datetime.datetime.now() - startTime
      metricsDF = appendToMetricsDF(metricsDF = metricsDF,
                                          lastLoadDts = "'{}'".format("null"), 
                                          FAILED = True,
                                          PHASE = "'{}'".format("PRE_PROCESS"), 
                                          SOURCE = "'{}'".format("updateSmartSheetMaintainedTAXOTables"), 
                                          TABLENAME = "'{}'".format("null"),
                                          FILENAME = "'{}'".format("null"), 
                                          PROCESSING_TIME = "'{}'".format(runTime), 
                                          RECORDS_READ = "'{}'".format("null"), 
                                          DUP_RECORDS = "'{}'".format("null"), 
                                          QUARANTINED_RECORDS = "'{}'".format("null"), 
                                          EXCEPTIONS = "'{}'".format("null"), 
                                          RECORDS_WRITTEN = "'{}'".format("null"))
    return all_exceptions