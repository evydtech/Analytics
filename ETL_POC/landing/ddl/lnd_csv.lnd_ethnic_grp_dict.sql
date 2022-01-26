-- Databricks notebook source

CREATE TABLE lnd_csv.lnd_ethnic_grp_dict (  
  ethnic_group_code STRING,
  long_desc STRING,
  short_desc STRING,
  race_code STRING

) USING PARQUET
LOCATION '/mnt/evydpocdata/ETL-poc/data/landing/lnd_csv/lnd_csv.lnd_ethnic_grp_dict';


-- COMMAND ----------


