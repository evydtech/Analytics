-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Only works for ORC

-- COMMAND ----------


-- CREATE TABLE lnd_csv.lnd_conditions (  
--   `start` DATE,
--   stop DATE,
--   patient STRING,
--   encounter STRING,
--   code LONG,
--   description STRING

-- ) USING ORC
-- LOCATION '/mnt/evydpocdata/ETL-poc/data/landing/lnd_csv/lnd_csv.lnd_conditions';



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Use this for Parquet

-- COMMAND ----------


CREATE TABLE lnd_csv.lnd_conditions (  
  `start` STRING,
  stop STRING,
  patient STRING,
  encounter STRING,
  code STRING,
  description STRING

) USING PARQUET
LOCATION '/mnt/evydpocdata/ETL-poc/data/landing/lnd_csv/lnd_csv.lnd_conditions';


-- COMMAND ----------


