-- Databricks notebook source

CREATE TABLE lnd_csv.lnd_mp_patient_anon (  
  patient_id STRING,
  sex STRING,
  date_of_birth STRING,
  race_code STRING,
  ethnic_grp_code STRING,
  deceased_date STRING,
  death_status_remarks STRING

) USING PARQUET
LOCATION '/mnt/evydpocdata/ETL-poc/data/landing/lnd_csv/lnd_csv.lnd_mp_patient_anon';


-- COMMAND ----------


