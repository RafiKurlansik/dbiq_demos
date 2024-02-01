# Databricks notebook source
# Define table and location
table_location = "richardt_demos.chicago_data.food_inspections"

# Define the API response CSV storage path:
api_resp_path = "/Volumes/richardt_demos/chicago_data/restaurant_data"

# Read the CSV file with schema inference from the first row
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(api_resp_path)

# Write the DataFrame to a Delta table and overwrite the existing data
df.write.format("delta").mode("overwrite").saveAsTable(table_location)

# COMMAND ----------

# DBTITLE 1,SQL Inspection Date Summary
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   count(*),
# MAGIC   min(inspection_date),
# MAGIC   max(inspection_date)
# MAGIC FROM richardt_demos.chicago_data.food_inspections
