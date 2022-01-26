-- Databricks notebook source

CREATE TABLE lnd_csv.lnd_pr_diagnosis_enc_dtl_anon (  
  patient_id STRING,
  term_code STRING,
  recorded_date_time STRING,
  status STRING,
  practitioner_id STRING,
  encounter_id STRING,
  term_code_short_desc STRING

) USING PARQUET
LOCATION '/mnt/evydpocdata/ETL-poc/data/landing/lnd_csv/lnd_csv.lnd_pr_diagnosis_enc_dtl_anon';


-- COMMAND ----------


