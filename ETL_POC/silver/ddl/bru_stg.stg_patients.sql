-- Databricks notebook source

CREATE TABLE bru_stg.stg_patients (
  stg_load_tms TIMESTAMP,
  
  id STRING,
  birthdate DATE,
  deathdate DATE,
  ssn STRING,
  drivers STRING,
  passport STRING,
  prefix STRING,
--   `first` STRING,
--   `last` STRING,
  full_name STRING,
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
  zip SHORT,
  lat DOUBLE,
  lon DOUBLE,
  healthcare_expenses DOUBLE,
  healthcare_coverage DOUBLE
  
) USING DELTA
LOCATION '/mnt/evydpocdata/ETL-poc/data/silver/bru_stg/bru_stg.stg_patients';


-- COMMAND ----------


