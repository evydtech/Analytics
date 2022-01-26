-- Databricks notebook source

CREATE TABLE bru_omop.omop_person (
  tgt_load_tms TIMESTAMP,
    
  person_id LONG,
  gender_concept_id LONG,
  year_of_birth VARCHAR(4),
  month_of_birth VARCHAR(2),
  day_of_birth VARCHAR(2),
  birth_datetime STRING,
  race_concept_id STRING,
  ethnicity_concept_id STRING,
  location_id STRING,
  provider_id STRING,
  care_site_id STRING,
  person_source_value STRING,
  gender_source_value CHAR(1),
  gender_source_concept_id STRING,
  race_source_value STRING,
  race_source_concept_id STRING,
  ethnicity_source_value STRING,
  ethnicity_source_concept_id STRING

) USING DELTA
LOCATION '/mnt/evydpocdata/ETL-poc/data/gold/bru_omop/bru_omop.omop_person';


-- COMMAND ----------


