# Databricks notebook source
##############################################################################
# Author: Low Han Siong
# Company: EVYD Tech
# Purpose: ETL bronze (skipped silver) to gold layer 
##############################################################################
# Version         Date            Author                Changelog
# 0.1             2021-11-19      Low Han Siong         initial draft
#
##############################################################################
# Source table
#   bru_wh.wh_pr_diagnosis_enc_dtl_anon_csv
#
# Target table
#   bru_omop.omop_condition_occurrence
#
##############################################################################

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import datetime
from delta.tables import *

##############################################################################
### sources
bronze_diagnosis = spark.table('bru_wh.wh_pr_diagnosis_enc_dtl_anon_csv').withColumn(
    'row_no', row_number().over(Window.partitionBy('src_id').orderBy(
        desc('src_load_tms'), desc('src_update_tms')))
).filter('row_no=1')

### transformations for omop.omop_condition_occurrence
    
# increment id - starts from 1, may need to increment from target table (if run multiple times insert)
gold_cond = bronze_diagnosis.withColumn(
    "idx", monotonically_increasing_id()
)
window = Window.orderBy(col('idx'))
gold_cond = gold_cond.withColumn('condition_occurrence_id', row_number().over(window))

# convert date to timestamp - nt sure why transform recorded_date_time since it will be transformed to date later
convert_date_tags = ['recorded_date_time']
for colm in convert_date_tags:
    gold_cond = gold_cond.withColumn(
        colm, col(colm).cast('timestamp')
    )
    
gold_cond = gold_cond.withColumnRenamed(
    'patient_id', 'person_id'
).withColumn(
    'condition_concept_id', col('term_code') # 2 columns using term_code?
).withColumn(
    'condition_source_concept_id', col('term_code')
).withColumn(
    'condition_start_date', to_date('recorded_date_time')
).withColumn(
    'condition_start_datetime', date_format('recorded_date_time', 'HH:mm:ss')
).withColumn(
    'condition_end_date', lit('')
).withColumn(
    'condition_end_datetime', lit('')
).withColumn(
    'condition_type_concept_id', lit('')
).withColumn(
    'stop_reason', lit('')
).withColumn(
    'visit_detail_id', lit('')
).withColumn(
    'condition_status_source_value', lit('')
).withColumnRenamed(
    'status','condition_status_concept_id'
).withColumnRenamed(
    'practitioner_id','provider_id'
).withColumnRenamed(
    'encounter_id','visit_occurence_id'
).withColumnRenamed(
    'term_code_short_desc','condition_source_value'
)

# audit column
gold_cond = gold_cond.withColumn(
    'tgt_load_tms', current_timestamp()
)

# upsert into target table
target_table = "bru_omop.omop_condition_occurrence"
target_file_path = spark.sql("desc formatted " + target_table).filter("col_name=='Location'").collect()[0].data_type
deltaTable = DeltaTable.forPath(spark, target_file_path)

deltaTable.alias("target").merge(
    gold_cond.alias("updates"),
    "target.person_id = updates.person_id") \
  .whenMatchedUpdateAll() \
  .whenNotMatchedInsertAll() \
  .execute()

# COMMAND ----------


