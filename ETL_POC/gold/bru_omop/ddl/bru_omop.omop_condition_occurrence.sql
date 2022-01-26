-- Databricks notebook source

CREATE TABLE bru_omop.omop_condition_occurrence (
  tgt_load_tms TIMESTAMP,
    
  condition_occurrence_id LONG,
  person_id STRING,
  condition_concept_id STRING,
  condition_start_date DATE,
  condition_start_datetime STRING,
  condition_end_date DATE,
  condition_end_datetime STRING,
  condition_type_concept_id STRING,
  condition_status_concept_id STRING,
  stop_reason STRING,
  provider_id STRING,
  visit_occurence_id STRING,
  visit_detail_id STRING,
  condition_source_value STRING,
  condition_source_concept_id STRING,
  condition_status_source_value STRING

) USING DELTA
LOCATION '/mnt/evydpocdata/ETL-poc/data/gold/bru_omop/bru_omop.omop_condition_occurrence';


-- COMMAND ----------


