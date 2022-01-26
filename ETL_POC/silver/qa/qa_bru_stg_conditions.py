# Databricks notebook source
##############################################################################
# Author: Low Han Siong
# Company: EVYD Tech
# Purpose: Validation check on bru_stg
##############################################################################
# Version         Date            Author                Changelog
# 0.1             2021-11-15      Low Han Siong         initial draft
#
##############################################################################
# Source table
#   bru_stg.stg_conditions
#
##############################################################################

import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window


# Validation Rules engine - potentially can build in .yaml

# 1) Input
job_name = "qa_bru_stg_conditions"
source_table = 'bru_stg.stg_conditions'
test_rules = [
        # original column, new column, rule
        ("start", "check_start_isnull", "start IS NOT NULL OR start != '' OR lower(start) != 'null'"),
        ("patient", "check_patient_isnull", "patient IS NOT NULL OR patient != '' OR lower(patient) != 'null'"),
        ("encounter", "check_encounter_isnull", "encounter IS NOT NULL OR encounter != '' OR lower(encounter) != 'null'")
]

# 2) Standard Rules Engine
df = spark.table(source_table)

for (ori_col, new_col, rule) in test_rules:
    
    cntstr = 'count_'
    cnt_col = cnt_col = cntstr + new_col
    
    df = df.withColumn(
        new_col, when(
            expr(rule) == True, lit(0)
        ).otherwise(lit(1))
    )
    
    cnt = df.select(sum(new_col)).collect()[0][0]
    
    df = df.withColumn(
        cnt_col, lit(cnt)
    )
    
cnt_cols = [colm for colm in df.columns if colm.startswith(cntstr)]
error_report = df.select(
    cnt_cols
).limit(1)

cnt_cols = [colm for colm in error_report.columns]
col_list = '+'.join(cnt_cols)

error_report_sum = error_report.withColumn(
    'total_error_count', expr(col_list)
).select(
    'total_error_count'
).collect()[0][0]

if error_report_sum != 0:
    print('--- Printing Error Report ---')
    display(error_report)
    raise Exception("Quality Check for " + source_table + " in " + job_name + " has failed.")
    
else:
    print('--- Quality Checks Passed ---')

# COMMAND ----------


