-- Databricks notebook source
-- Full load table
CREATE TABLE bru_wh.wh_ethnic_grp_dict_csv (
  src_partition DATE,
  src_load_tms TIMESTAMP,
  src_remark STRING,
  src_source STRING,
  
  ethnic_group_code STRING,
  long_desc STRING,
  short_desc STRING,
  race_code STRING

) USING DELTA
PARTITIONED BY (src_partition)
LOCATION '/mnt/evydpocdata/ETL-poc/data/bronze/bru_wh/bru_wh.wh_ethnic_grp_dict_csv';



-- COMMAND ----------

select * from bru_wh.wh_race_code_dict_csv

-- COMMAND ----------


