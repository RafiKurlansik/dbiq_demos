# Databricks notebook source
table_location = "richardt_demos.chicago_data.food_inspections"
api_resp_path = "/Volumes/richardt_demos/chicago_data/restaurant_data"
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(api_resp_path)
df.write.format("delta").mode("overwrite").saveAsTable(table_location)
