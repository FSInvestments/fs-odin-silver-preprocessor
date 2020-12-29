# Databricks notebook source
## Import necessary Python libraries and functions ##
import requests
import json
import hashlib
import re
import datetime
import base64
import pandas
from pandas.io.json import json_normalize
from pandas.util import hash_pandas_object
from pyspark.sql.functions import col, concat, concat_ws, lit, current_timestamp, to_timestamp
from pyspark.sql.functions import monotonically_increasing_id, md5
from pyspark.sql.types import StringType, StructType, StructField, TimestampType, BooleanType, IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC #### RUN EXTERNAL NOTEBOOKS ####

# COMMAND ----------

# MAGIC %run
# MAGIC ../../odin_utilities/utilities/ODIN_UTILS

# COMMAND ----------

# MAGIC %run
# MAGIC ../../odin_utilities/utilities/APPEND_TO_METRICS

# COMMAND ----------

# MAGIC %run
# MAGIC ../../odin_utilities/utilities/SALESFORCE_CONNECTION

# COMMAND ----------

# MAGIC %run
# MAGIC ../../odin_utilities/utilities/SALESFORCE_CASE_v3

# COMMAND ----------

# MAGIC %run
# MAGIC ../../odin_utilities/utilities/SALESFORCE_POST_EXCEPTION_METRICS

# COMMAND ----------

# MAGIC %run
# MAGIC ../../odin_utilities/utilities/POST_PROCESSOR

# COMMAND ----------

# MAGIC %run
# MAGIC ../../odin_utilities/utilities/SMARTSHEET_ADAPTER

# COMMAND ----------

# MAGIC %run
# MAGIC ../../odin_utilities/utilities/SNOWFLAKE

# COMMAND ----------

# MAGIC %run
# MAGIC ../../odin_utilities/utilities/SCHEMA_FUNCTIONS

# COMMAND ----------

# MAGIC %run 
# MAGIC ./SILVER_PREPROCESSOR_FUNCTIONS

# COMMAND ----------

# MAGIC %md
# MAGIC #### Execute Pre-Processor ####

# COMMAND ----------

preProcessorJobStartTime = datetime.datetime.now()
print('ODIN PREPROCESSOR STARTED')

## Set database and environment variables ##
tgtEnv = dbutils.widgets.get("tgtEnv").lower()
excludeTaxos = False

if tgtEnv == "qa" or tgtEnv == "dev":
  dbPrefix = "{}_odin".format(tgtEnv)
  snowflakeScope = "odin-ingest-{}".format(tgtEnv)
  snowflakeRole = "ODIN_SVC_INGEST_{}".format(tgtEnv)
  snowflakeWarehouse = "REFINERY_INGEST_{}".format(tgtEnv)
  snowflakeDatabase = "ODIN_{}".format(tgtEnv)
  referenceDB = "{}_ODIN_REFERENCE".format(tgtEnv)
  jobDB = '{}_ODIN_JOB_TABLES'.format(tgtEnv)
  metricsDB = '{}_ODIN_JOB_METRICS'.format(tgtEnv)
  exceptionDB = '{}_ODIN_EXCEPTIONS'.format(tgtEnv)
  quarantineDB = '{}_ODIN_QUARANTINE'.format(tgtEnv)
  silverDB = 'ODIN_{}'.format(tgtEnv)
  
else:
  dbPrefix = "odin_{}".format(tgtEnv)
  snowflakeScope = "odin-ingest"
  snowflakeRole = "ODIN_SVC_INGEST_PROD"
  snowflakeWarehouse = "REFINERY_INGEST_PROD"
  snowflakeDatabase = "ODIN"
  referenceDB = "ODIN_REFERENCE"
  jobDB = 'ODIN_JOB_TABLES'
  metricsDB = "ODIN_JOB_METRICS"
  exceptionDB = 'ODIN_EXCEPTIONS'
  quarantineDB = 'ODIN_QUARANTINE'
  if tgtEnv == 'invops':
    silverDB = "ODIN_INV_OPS"
  else:
    silverDB = "{}".format(dbPrefix)
  
bronzeDB = "{}_bronze".format(dbPrefix)

refSnowflake = snowSql(role = snowflakeRole, warehouse = snowflakeWarehouse, schema = "REFERENCE", database = snowflakeDatabase, snowflakeScope = snowflakeScope)
silverSnowflake = snowSql(role = snowflakeRole, warehouse = snowflakeWarehouse, schema = "SILVER_STAGING", database = silverDB, snowflakeScope = snowflakeScope)

## Make sure that job table for ref table hashes exists for the job ##
spark.sql("CREATE TABLE IF NOT EXISTS {jobDB}.ref_hash_key_map (smartsheetName string, hashKey string) USING DELTA PARTITIONED BY (smartsheetName)".format(jobDB = jobDB))

## Creates exception df with exceptions schema ##
all_exceptions_schema = StructType([StructField("FS_EXCEPTION_ID", StringType()),
                                    StructField("FS_POST_ID", StringType()),
                                    StructField("CASE_MANAGEMENT_ID", StringType()),
                                    StructField("SUBJECT", StringType()),
                                    StructField("DESCRIPTION", StringType()),
                                    StructField("RECORD_TYPE_ID", StringType()),
                                    StructField("OWNER_ID", StringType()),
                                    StructField("PIPELINE", StringType()),
                                    StructField("ORIGIN", StringType()),
                                    StructField("EXCEPTION_ID_TYPE", StringType()),
                                    StructField("EXCEPTION_SOURCE", StringType()),
                                    StructField("EXCEPTION_STATUS", StringType()),
                                    StructField("PRIORITY", StringType()),
                                    StructField("EXCEPTION_TYPE", StringType()),
                                    StructField("EXCEPTION_SUB_TYPE", StringType()),
                                    StructField("SOURCE_LINK", StringType()),
                                    StructField("FILE_CREATION_DTS", TimestampType()),
                                    StructField("EXCEPTION_DTS", TimestampType())])
all_exceptions = spark.createDataFrame([], schema=all_exceptions_schema)
all_exceptions.createOrReplaceGlobalTempView("all_exceptions")

# Create blank `metricsDF` ##
metricsDFschema = StructType([StructField("PROCESSING_DATE", TimestampType()),
                              StructField("FAILED", BooleanType()),
                              StructField("REFINERY_PHASE", StringType()),
                              StructField("DATA_SOURCE", StringType()),
                              StructField("TABLENAME", StringType()),
                              StructField("FILENAME", StringType()),
                              StructField("PROCESSING_TIME", TimestampType()),
                              StructField("RECORDS_READ", IntegerType()),
                              StructField("DUPLICATE_RECORDS", IntegerType()),
                              StructField("QUARANTINED_RECORDS", IntegerType()),
                              StructField("EXCEPTIONS", IntegerType()),
                              StructField("RECORDS_WRITTEN", IntegerType())])
metricsDF = spark.createDataFrame([], schema = metricsDFschema)

## Initialize local dictionaries/lists needed ##
preProcessorLocal = {}
refSheetIds = {}
snowflakeScheduleOfWork = ['ref_instrument_issuer_keys']

## Environment variables ##
smart_sheet_access_token = dbutils.secrets.get(scope = "fs-smartsheet", key = "token")

## RUN FUNCTIONS ##
## Pull Configuration Smartsheets ##
print("Running: pullSmartsheetPreProcessorSheets")
pullSmartsheetPreProcessorSheets(smart_sheet_access_token, tgtEnv, excludeTaxos)

## Process Smartsheet maintained reference tables ##
print("Running: updateSmartSheetMaintainedRefTables")
all_exceptions = updateSmartSheetMaintainedRefTables(preProcessorLocal, refSheetIds, referenceDB, silverDB, jobDB, tgtEnv, all_exceptions, metricsDF)

## Process Smartsheet maintained TAXO tables ##
print("Running: updateSmartSheetMaintainedTAXOTables")
all_exceptions = updateSmartSheetMaintainedTAXOTables(preProcessorLocal, refSheetIds, silverDB, jobDB, tgtEnv, all_exceptions, metricsDF)

## Run PIVOT functions. Necessary prep for updateAutomaticallyMaintainedRefTables ##
print("Running: create_reference_pivot_views")
all_exceptions = create_reference_pivot_views(bronzeDB, all_exceptions, tgtEnv, metricsDF)

## Process self-maintaining reference tables ##
print("Running: updateAutomaticallyMaintainedRefTables")
all_exceptions = updateAutomaticallyMaintainedRefTables(preProcessorLocal, bronzeDB, referenceDB, tgtEnv, all_exceptions, metricsDF)

## Create split and concat files in bronze ##
print("Running: split_bronze_files")
all_exceptions = split_bronze_files(referenceDB, bronzeDB, all_exceptions, metricsDF, tgtEnv)

## Update HUB tables from config sheet ##
print("Running: updateHubTable")
all_exceptions = updateHubTable(referenceDB, silverDB, silverSnowflake, tgtEnv, all_exceptions, metricsDF)

## Append tables to Snowflake ##
print('Running: append_ref_tables_to_snowflake')
all_exceptions = append_ref_tables_to_snowflake(refSnowflake, referenceDB, snowflakeScheduleOfWork, all_exceptions, tgtEnv, metricsDF)

# COMMAND ----------

## Optimize tables from split_files function ##
print('Running: optimize_split_file_tables')
all_exceptions = optimize_split_file_tables(all_exceptions, metricsDF, tgtEnv)

# COMMAND ----------

## Send exceptions to Delta Table ##
print("Running: insertIntoExceptionsTbl")
# Failing on driver/broadcast join despite only being 15 rows ##
insertIntoExceptionsTbl(all_exceptions, tgtEnv)
print("ODIN PREPROCESSOR COMPLETED")
preProcessorJobEndTime = datetime.datetime.now()

# COMMAND ----------

dbutils.notebook.exit("ODIN Preprocessor has completed successfully in {tgtEnv}".format(tgtEnv = tgtEnv))