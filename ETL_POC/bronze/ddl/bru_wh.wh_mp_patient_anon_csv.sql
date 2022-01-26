-- Databricks notebook source
-- Append table

CREATE TABLE bru_wh.wh_mp_patient_anon_csv (
  src_id STRING,
  src_load_tms TIMESTAMP,
  src_update_tms TIMESTAMP,
  src_event CHAR(1),
  src_remark STRING,
  src_source STRING,
  
  patient_id STRING,
  sex STRING,
  date_of_birth DATE,
  race_code STRING,
  ethnic_grp_code STRING,
  deceased_date DATE,
  death_status_remarks STRING
  
) USING DELTA
LOCATION '/mnt/evydpocdata/ETL-poc/data/bronze/bru_wh/bru_wh.wh_mp_patient_anon_csv';


-- COMMAND ----------

select src_load_tms, src_update_tms, src_event, src_remark, src_source, patient_id from bru_wh.wh_mp_patient_anon_csv where src_id = '2fac1de2a78b034892bdf3dab1bf5b87a1206af2bdebe2161cd389cb173a4665064ec89ce7103f0467e271b1af14bf7c'

-- COMMAND ----------


