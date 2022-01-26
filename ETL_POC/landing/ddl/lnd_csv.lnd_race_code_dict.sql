-- Databricks notebook source

CREATE TABLE lnd_csv.lnd_race_code_dict (  
  race_code STRING,
  long_desc STRING,
  short_desc STRING

) USING PARQUET
LOCATION '/mnt/evydpocdata/ETL-poc/data/landing/lnd_csv/lnd_csv.lnd_race_code_dict';


-- COMMAND ----------


