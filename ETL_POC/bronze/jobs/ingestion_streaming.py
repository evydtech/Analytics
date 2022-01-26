# Databricks notebook source
# MAGIC %fs ls /databricks-datasets/structured-streaming/events/

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

inputPath = "/databricks-datasets/structured-streaming/events/"

# Define the schema to speed up processing
jsonSchema = StructType([ StructField("time", TimestampType(), True), StructField("action", StringType(), True) ])

streamingInputDF = (
  spark
    .readStream
    .schema(jsonSchema)               # Set the schema of the JSON data
    .option("maxFilesPerTrigger", 1)  # Treat a sequence of files as a stream by picking one file at a time
    .json(inputPath)
)

streamingCountsDF = (
  streamingInputDF
    .groupBy(
      streamingInputDF.action,
      window(streamingInputDF.time, "1 hour"))
    .count()
)

query = (
  streamingCountsDF
    .writeStream
    .format("memory")        # memory = store in-memory table (for testing only)
    .queryName("counts")     # counts = name of the in-memory table
    .outputMode("complete")  # complete = all the counts should be in the table
    .start()
)

hdfhdfh

# COMMAND ----------


