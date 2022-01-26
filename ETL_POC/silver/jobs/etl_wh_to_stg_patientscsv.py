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
#   bru_wh.wh_patients_csv
#
# Target table
#   bru_stg.stg_patients
#
##############################################################################

import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# initialise audit timestamp column
current_run = str(datetime.datetime.now().date())
dt_current_run = datetime.datetime.strptime(current_run, '%Y-%m-%d').date()
#dt_start = dt_current_run.replace(day=1)

df = spark.table('bru_wh.wh_patients_csv')

# get max partition from Full Load table
df = df.filter(
    col('src_partition') == df.agg({"src_partition": "max"}).collect()[0][0]
)

# remove numbers from names & create new full_name col
df = df.withColumn(
    'first', regexp_replace('first', r'[0-9]', '')
).withColumn(
    'last', regexp_replace('last', r'[0-9]', '')
).withColumn(
    'full_name', concat_ws(' ', col('first'), col('last'))
)


# de-anonymization columns with PII - salting with token simulation
pii_cols = {'full_name': 'fs425','passport': '324njf','address': '123f1','city': '12sdr'}

for cols,salt in pii_cols.items():
    df = df.withColumn(
        cols,
        sha2(concat_ws('',cols,lit(salt)), 256)
    )

# hash patient id for joins- take it as it is IC number
salt = '21vvz'
df = df.withColumn(
    'id', sha2(concat_ws('','id', lit(salt)), 256)
)

# audit column
df = df.withColumn(
    'stg_load_tms', current_timestamp()
)

# writing to final table - can do upsert merge if necessary
target_name = 'bru_stg.stg_patients'

df.select(spark.table(target_name).columns).write.format("delta").mode("overwrite").insertInto(target_name, overwrite = True)

# COMMAND ----------


