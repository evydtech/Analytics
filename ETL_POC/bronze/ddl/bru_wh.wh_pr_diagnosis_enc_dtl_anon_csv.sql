-- Databricks notebook source
-- Append table

CREATE TABLE bru_wh.wh_pr_diagnosis_enc_dtl_anon_csv (
  src_id STRING,
  src_load_tms TIMESTAMP,
  src_update_tms TIMESTAMP,
  src_event char(1),
  src_remark STRING,
  src_source STRING,
  
  patient_id STRING,
  term_code STRING,
  recorded_date_time TIMESTAMP,
  status STRING,
  practitioner_id STRING,
  encounter_id STRING,
  term_code_short_desc STRING
  
) USING DELTA
LOCATION '/mnt/evydpocdata/ETL-poc/data/bronze/bru_wh/bru_wh.wh_pr_diagnosis_enc_dtl_anon_csv';


-- COMMAND ----------


