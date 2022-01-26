-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Only works for ORC

-- COMMAND ----------


-- CREATE TABLE lnd_csv.lnd_patients (  
--   id STRING,
--   birthdate DATE,
--   deathdate DATE,
--   ssn STRING,
--   drivers STRING,
--   passport STRING,
--   prefix STRING,
--   `first` STRING,
--   `last` STRING,
--   suffix STRING,
--   maiden STRING,
--   marital STRING,
--   race STRING,
--   ethnicity STRING,
--   gender STRING,
--   birthplace STRING,
--   address STRING,
--   city STRING,
--   state STRING,
--   county STRING,
--   zip SHORT,
--   lat DOUBLE,
--   lon DOUBLE,
--   healthcare_expenses DOUBLE,
--   healthcare_coverage DOUBLE

-- ) USING ORC
-- LOCATION '/mnt/evydpocdata/ETL-poc/data/landing/lnd_csv/lnd_csv.lnd_patients';



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Use this for PARQUET

-- COMMAND ----------


CREATE TABLE lnd_csv.lnd_patients (  
  id STRING,
  birthdate STRING,
  deathdate STRING,
  ssn STRING,
  drivers STRING,
  passport STRING,
  prefix STRING,
  `first` STRING,
  `last` STRING,
  suffix STRING,
  maiden STRING,
  marital STRING,
  race STRING,
  ethnicity STRING,
  gender STRING,
  birthplace STRING,
  address STRING,
  city STRING,
  state STRING,
  county STRING,
  zip STRING,
  lat STRING,
  lon STRING,
  healthcare_expenses STRING,
  healthcare_coverage STRING

) USING PARQUET
LOCATION '/mnt/evydpocdata/ETL-poc/data/landing/lnd_csv/lnd_csv.lnd_patients';



-- COMMAND ----------


