# Databricks notebook source
# can try auto loader if don't need to keep history

import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import json

# initialise audit timestamp column
current_run = str(datetime.datetime.now().date())
dt_current_run = datetime.datetime.strptime(current_run, '%Y-%m-%d').date()
#dt_start = dt_current_run.replace(day=1)

# spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
# spark.conf.set('hive.exec.dynamic.partition.mode','nonstrict')
################################################################################

# databases
target_db = "bru_wh"

#####################
#### APPEND ONLY ####
#####################

# # list of tables (can give as parameters maybe in .sh file or .yaml file or ADF flow?)
# src_tgt_mapping = {
#     'lnd_csv.lnd_mp_patient_anon': 'wh_mp_patient_anon_csv',
#     'lnd_csv.lnd_pr_diagnosis_enc_dtl_anon': 'wh_pr_diagnosis_enc_dtl_anon_csv'
# }

# Parse in table_list based on Databricks Jobs parameters
src_list = getArgument("src_list")
result = dbutils.notebook.run(src_list, 60)
src_tgt_mapping = json.loads(result)

for src, tgt in src_tgt_mapping.items():
    source_db, src_tbl = src.split('.')
    source = source_db.split('_')[1].upper()
    src_df = spark.table(src)

    # initialising composite key (parameter in other file)
    pk_ids = "patient_id"
    update_tms = "date_recorded"
    
    pk_str = ["trim(cast({} AS STRING))".format(pk) for pk in pk_ids.split(',')]
    pk_expr = "sha2(concat_ws('|',{}), 384)".format(', '.join(pk_str))
    
    # Adding audit columns (note time is changed to GMT+8 - in spark config)
    df = src_df.withColumn(
        'src_id', expr(pk_expr)
    ).withColumn(
        'src_load_tms', current_timestamp()
    ).withColumn(
        'src_update_tms', current_timestamp() # can be last modified date from data
    ).withColumn(
        'src_event', lit('I') #I: Insert, U: Update, D: Delete -> depending on source CDC
    ).withColumn(
        'src_remark', lit('Delta Daily Load')
    ).withColumn(
        'src_source', lit(source)
    )

    # simple count validation check
    if src_df.count() == df.count():
        print('COUNT of ' + src_tbl + ' is: ' + str(df.count()))
        
        # schema mapping using target DDL
        for colname, datatype in spark.table(target_db + '.' + tgt).dtypes:
            df = df.withColumn(
                colname, col(colname).cast(datatype)
            )
            
        # replace partition using replaceWhere instead of dynamicPartitioning
        df.write.format("delta").mode("append").saveAsTable(target_db + '.' + tgt)
       
        tgt = spark.table(target_db + '.' + tgt)
    else:
        print('Count of ' + source_db + '.' + src_tbl + ' does not match with ' + target_db + '.' + tgt)
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time Travel / Versioning

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- note that data / log files are captured and kept in data even though we overwrite them, therefore, we need to configure how long is the retention policy so as not to keep too much data
# MAGIC 
# MAGIC -- ALTER table_name SET TBLPROPERTIES ('delta.logRetentionDuration'='interval 240 hours', 'delta.deletedFileRetentionDuration'='interval 1 hours')
# MAGIC -- SHOW TBLPROPERTIES table_name
