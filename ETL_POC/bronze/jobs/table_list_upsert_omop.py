# Databricks notebook source
import json
dbutils.notebook.exit(json.dumps({
   'lnd_csv.lnd_mp_patient_anon': 'wh_mp_patient_anon_csv',
   'lnd_csv.lnd_pr_diagnosis_enc_dtl_anon': 'wh_pr_diagnosis_enc_dtl_anon_csv'
}))


# COMMAND ----------


