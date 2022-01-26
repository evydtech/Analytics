-- Databricks notebook source
-- Full load table
CREATE TABLE bru_wh.wh_race_code_dict_csv (
  src_partition DATE,
  src_load_tms TIMESTAMP,
  src_remark STRING,
  src_source STRING,

  race_code STRING,
  long_desc STRING,
  short_desc STRING

) USING DELTA
PARTITIONED BY (src_partition)
LOCATION '/mnt/evydpocdata/ETL-poc/data/bronze/bru_wh/bru_wh.wh_race_code_dict_csv';



-- COMMAND ----------


