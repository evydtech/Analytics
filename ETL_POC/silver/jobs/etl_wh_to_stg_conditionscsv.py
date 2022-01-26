# Databricks notebook source
##############################################################################
# Author: Low Han Siong
# Company: EVYD Tech
# Purpose: ETL bronze to silver layer 
##############################################################################
# Version         Date            Author                Changelog
# 0.1             2021-11-15      Low Han Siong         initial draft
#
##############################################################################
# Source table
#   bru_wh.wh_conditions_csv
#
# Target table
#   bru_stg.stg_conditions
#
##############################################################################

import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# initialise audit timestamp column
current_run = str(datetime.datetime.now().date())
dt_current_run = datetime.datetime.strptime(current_run, '%Y-%m-%d').date()

df = spark.table('bru_wh.wh_conditions_csv')

# get max partition from Full Load table
df = df.filter(
    col('src_partition') == df.agg({"src_partition": "max"}).collect()[0][0]
)

# hash patient id for joins- take it as it is IC number
salt = '21vvz'
df = df.withColumn(
    'patient', sha2(concat_ws('','patient', lit(salt)), 256)
)

# audit column
df = df.withColumn(
    'stg_load_tms', current_timestamp()
)

# writing to final table - can do upsert merge if necessary
target_name = 'bru_stg.stg_conditions'

df.select(spark.table(target_name).columns).write.format("delta").mode("overwrite").insertInto(target_name, overwrite = True)


# COMMAND ----------


