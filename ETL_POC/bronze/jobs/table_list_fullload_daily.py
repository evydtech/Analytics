# Databricks notebook source
import json
dbutils.notebook.exit(json.dumps({
  "lnd_csv.lnd_patients": "wh_patients_csv",
  "lnd_csv.lnd_conditions": "wh_conditions_csv"
}))


# COMMAND ----------


