-- Databricks notebook source

CREATE TABLE bru_tgt.tgt_active_patients_cond (
  tgt_load_tms TIMESTAMP,
    
  id STRING,
  full_name STRING,
  current_age INT,
  passport STRING,
  marital CHAR(1),
  race_coded CHAR(1),
  ethnicity VARCHAR(20),
  gender CHAR(1),
  address STRING,
  city STRING,
  conditions STRING

) USING DELTA
LOCATION '/mnt/evydpocdata/ETL-poc/data/gold/bru_tgt/bru_tgt.tgt_active_patients_cond';


-- COMMAND ----------

describe history bru_tgt.tgt_active_patients_cond

-- COMMAND ----------

SELECT * FROM bru_tgt.tgt_active_patients_cond TIMESTAMP AS OF '2021-11-17T14:53:48.000'

-- COMMAND ----------

select src_partition, count(*) from bru_wh.wh_patients_csv group by src_partition

-- COMMAND ----------


