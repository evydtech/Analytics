-- Databricks notebook source

CREATE TABLE bru_stg.stg_conditions (
  
  stg_load_tms TIMESTAMP,
  
  `start` DATE,
  stop DATE,
  patient STRING,
  encounter STRING,
  code LONG,
  description STRING

) USING DELTA
LOCATION '/mnt/evydpocdata/ETL-poc/data/silver/bru_stg/bru_stg.stg_conditions';



-- COMMAND ----------


