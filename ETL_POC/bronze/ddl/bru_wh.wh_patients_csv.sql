-- Databricks notebook source
-- Full load table

CREATE TABLE bru_wh.wh_patients_csv (
  src_partition DATE,
  src_load_tms TIMESTAMP,
  src_remark STRING,
  src_source STRING,
  
  id STRING,
  birthdate DATE,
  deathdate DATE,
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
  zip SHORT,
  lat DOUBLE,
  lon DOUBLE,
  healthcare_expenses DOUBLE,
  healthcare_coverage DOUBLE
  
) USING DELTA
PARTITIONED BY (src_partition)
LOCATION '/mnt/evydpocdata/ETL-poc/data/bronze/bru_wh/bru_wh.wh_patients_csv';


-- COMMAND ----------


