-- Databricks notebook source
-- Full load table
CREATE TABLE bru_wh.wh_conditions_csv (
  src_partition DATE,
  src_load_tms TIMESTAMP,
  src_remark STRING,
  src_source STRING,
  
  `start` DATE,
  stop DATE,
  patient STRING,
  encounter STRING,
  code LONG,
  description STRING

) USING DELTA
PARTITIONED BY (src_partition)
LOCATION '/mnt/evydpocdata/ETL-poc/data/bronze/bru_wh/bru_wh.wh_conditions_csv';



-- COMMAND ----------


