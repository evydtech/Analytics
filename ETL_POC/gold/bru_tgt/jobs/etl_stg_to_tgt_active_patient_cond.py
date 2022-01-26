# Databricks notebook source
##############################################################################
# Author: Low Han Siong
# Company: EVYD Tech
# Purpose: ETL silver to gold layer 
##############################################################################
# Version         Date            Author                Changelog
# 0.1             2021-11-15      Low Han Siong         initial draft
#
##############################################################################
# Source table
#   bru_stg.stg_patients
#   bru_stg.stg_conditions
#
# Target table
#   bru_tgt.tgt_active_patients_cond
#
##############################################################################

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import datetime

# sources
patients = spark.table('bru_stg.stg_patients')
conditions = spark.table('bru_stg.stg_conditions')


# Objective: Finding out alive patients with active chronic illnesses

# 1) Cleaning up patients data - get cleaned PII info of alive patients

# Filter for alive patients
patients2 = patients.filter(
    col('deathdate').isNull()
)

# Converting race to single character e.g. white --> W
patients2 = patients2.withColumn(
    'race_coded', upper(col('race').substr(1,1))
)

# Calculate current age
patients2 = patients2.withColumn(
    'current_age', (months_between(current_date(),col("birthdate"))/12).cast('int')
)

# Columns selection
patients3 = patients2.select(
    'id', 
    'full_name',
    'current_age',
    'passport',
    'marital',
    'race_coded',
    'ethnicity',
    'gender',
    'address',
    'city'
)

# 2) Cleaning up conditions data - identifying patients with active serious/ chronic illnesses

# Remove words in parentheses
conditions2 = conditions.withColumn(
    'conditions', regexp_replace('description',"\(.*\)","")
)

# Remove special characters
conditions2 = conditions2.withColumn(
    'conditions', trim(lower(regexp_replace('conditions', "^A-Z0-9_]", " ")))
)

# Finding out patients with existing serious illnesses
symptoms = ['chronic', 'acute', 'heart', 'cancer']

conditions2 = conditions2.filter(
    (col('conditions').rlike("|".join(["(" + symptom + ")" for symptom in symptoms]))) & (col('stop').isNull())
)

# Concat symptoms into 1 array
conditions3 = conditions2.groupBy(
    'patient'
).agg(
    collect_list('conditions').alias('conditions')
).select(
    col('patient').alias('id'),
    'conditions'
)

# 3) Joining patients and conditions to create main df
# main join (inner join to get ALIVE patients with ACTIVE conditions)
main_df = patients3.alias('p').join(
    conditions3.alias('c'), 'id' , 'inner'
)

# audit column
main_df = main_df.withColumn(
    'tgt_load_tms', current_timestamp()
)
    
# writing to final table - can do upsert merge if necessary
target_name = 'bru_tgt.tgt_active_patients_cond'

# main_df.select(spark.table(target_name).columns).write.format("delta").mode("overwrite").insertInto(target_name, overwrite = True)
main_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC --We recommend you start by running OPTIMIZE on a daily basis (preferably at night when spot prices are low). Then modify your job from there.
# MAGIC -- optimize  default.active_patients_conditions_sanitized
# MAGIC -- vacuum -> remove transaction logs - save space

# COMMAND ----------


