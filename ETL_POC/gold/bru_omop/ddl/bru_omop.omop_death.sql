-- Databricks notebook source

CREATE TABLE bru_omop.omop_death (
  tgt_load_tms TIMESTAMP,
    
  person_id STRING,
  death_date DATE,
  death_datetime STRING,
  death_type_concept_id STRING,
  cause_concept_id STRING,
  cause_source_value STRING,
  cause_source_concept_id STRING

) USING DELTA
LOCATION '/mnt/evydpocdata/ETL-poc/data/gold/bru_omop/bru_omop.omop_death';


-- COMMAND ----------


