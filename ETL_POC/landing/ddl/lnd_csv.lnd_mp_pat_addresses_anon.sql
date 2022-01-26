-- Databricks notebook source

CREATE TABLE lnd_csv.lnd_mp_pat_addresses_anon (  
  patient_id STRING,
  addr1_line1 STRING,
  addr1_line2 STRING,
  addr1_line3 STRING,
  addr1_line4 STRING,
  postal1_code STRING

) USING PARQUET
LOCATION '/mnt/evydpocdata/ETL-poc/data/landing/lnd_csv/lnd_csv.lnd_mp_pat_addresses_anon';


-- COMMAND ----------


