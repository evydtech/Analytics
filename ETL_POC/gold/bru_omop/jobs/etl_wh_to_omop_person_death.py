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
#   bru_wh.wh_mp_patient_anon_csv
#   bru_wh.wh_ethnic_grp_dict_csv 
#   bru_wh.wh_race_code_dict_csv
#
# Target table
#   bru_omop.omop_person
#   bru_omop.omop_death
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
bronze_patient = spark.table('bru_wh.wh_mp_patient_anon_csv').withColumn(
    'row_no', row_number().over(Window.partitionBy('src_id').orderBy(
        desc('src_load_tms'), desc('src_update_tms')))
).filter('row_no=1')

bronze_ethnic_dict = spark.table('bru_wh.wh_ethnic_grp_dict_csv')
bronze_ethnic_dict = bronze_ethnic_dict.filter(
    col('src_partition') == bronze_ethnic_dict.agg({"src_partition": "max"}).collect()[0][0]
)

bronze_race_dict = spark.table('bru_wh.wh_race_code_dict_csv')
bronze_race_dict = bronze_race_dict.filter(
    col('src_partition') == bronze_race_dict.agg({"src_partition": "max"}).collect()[0][0]
)

# hardcoded values - can store in csv etc
gender_map_id = {
    "M": 8507,
    "F": 8532
}
    
### transformations for omop.omop_person
# convert date to timestamp - nt sure why transform deceased_date since it will be transformed to date later
convert_date_tags = ['deceased_date', 'date_of_birth']
for colm in convert_date_tags:
    bronze_patient = bronze_patient.withColumn(
        colm, col(colm).cast('timestamp')
    )

# fillna
gold_patient = bronze_patient.na.fill('')

# increment id - starts from 1, may need to increment from target table (if run multiple times insert)
gold_patient = gold_patient.withColumn(
    "idx", monotonically_increasing_id()
)
window = Window.orderBy(col('idx'))
gold_patient = gold_patient.withColumn('person_id', row_number().over(window))

# map gender
def map_gender(colm):
    return gender_map_id[colm]

map_gender_udf = udf(map_gender, LongType())

gold_patient = gold_patient.withColumn(
    'gender_concept_id', map_gender_udf('sex')
)

# date formating
gold_patient = gold_patient.withColumn(
    'year_of_birth', year('date_of_birth')
).withColumn(
    'month_of_birth', month('date_of_birth')
).withColumn(
    'day_of_birth', dayofmonth('date_of_birth')
).withColumn(
    'birth_datetime', date_format('date_of_birth', 'HH:mm:ss')
)

gold_patient = gold_patient.withColumnRenamed(
    #existing , new
    'race_code', 'race_concept_id'
).withColumnRenamed(
    'ethnic_grp_code', 'ethnicity_concept_id'
).withColumn(
    'location_id', lit('')
).withColumn(
    'provider_id', lit('')
).withColumn(
    'care_site_id', lit('')
).withColumnRenamed( 
    'patient_id', 'person_source_value'
).withColumnRenamed(
    'sex', 'gender_source_value'
).withColumn(
    'gender_source_concept_id', lit('')
).withColumn(
    'race_source_concept_id', lit('')
).withColumn(
    'ethnicity_source_value', lit('')
)

# lookup race
gold_patient = gold_patient.alias('main').join(
    bronze_race_dict.alias('race'), col('main.race_concept_id') == col('race.race_code'), 'left'
).select(
    'main.*',
    'race.long_desc'
).withColumnRenamed(
    'long_desc', 'race_source_value'
)

# lookup ethnic
gold_patient = gold_patient.alias('main').join(
    bronze_ethnic_dict.alias('ethnic'), col('main.ethnicity_concept_id') == col('ethnic.ethnic_group_code'), 'left'
).select(
    'main.*',
    'ethnic.long_desc'
).withColumnRenamed(
    'long_desc', 'ethnicity_source_concept_id'
)

# audit column
gold_patient = gold_patient.withColumn(
    'tgt_load_tms', current_timestamp()
)

# upsert into target table
target_table = "bru_omop.omop_person"
target_file_path = spark.sql("desc formatted " + target_table).filter("col_name=='Location'").collect()[0].data_type
deltaTable = DeltaTable.forPath(spark, target_file_path)

deltaTable.alias("target").merge(
    gold_patient.alias("updates"),
    "target.person_source_value = updates.person_source_value") \
  .whenMatchedUpdateAll() \
  .whenNotMatchedInsertAll() \
  .execute()

### transformations for omop.omop_death

gold_death = bronze_patient.withColumnRenamed(
    'patient_id', 'person_id'
).withColumn(
    'death_date', to_date('deceased_date')
).withColumn(
    #datetime doesnt mean timestamp, instead it is time??
    'death_datetime', date_format('deceased_date', 'HH:mm:ss')
).withColumn(
    'death_type_concept_id', lit('')
).withColumn(
    'cause_concept_id', lit('')
).withColumnRenamed(
    'death_status_remarks', 'cause_source_value'
).withColumn(
    'cause_source_concept_id', lit('')
)

# audit column
gold_death = gold_death.withColumn(
    'tgt_load_tms', current_timestamp()
)

# upsert into target table
target_table = "bru_omop.omop_death"
target_file_path = spark.sql("desc formatted " + target_table).filter("col_name=='Location'").collect()[0].data_type
deltaTable = DeltaTable.forPath(spark, target_file_path)

deltaTable.alias("target").merge(
    gold_death.alias("updates"),
    "target.person_id = updates.person_id") \
  .whenMatchedUpdateAll() \
  .whenNotMatchedInsertAll() \
  .execute()

# COMMAND ----------


