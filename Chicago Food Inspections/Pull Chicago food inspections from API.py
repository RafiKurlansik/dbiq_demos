# Databricks notebook source
# DBTITLE 1,Python Code for Importing Data with Timestamps
# Databricks notebook source
import os, time
import requests
import json
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,API JSON Storage Path
# Define the API response CSV storage path:
api_resp_path = "/Volumes/richardt_demos/chicago_data/restaurant_data"

# COMMAND ----------

# DBTITLE 1,Directory Deletion and Recreation
# Delete the directory:
dbutils.fs.rm(api_resp_path,True)
# Remake the dir:
dbutils.fs.mkdirs(api_resp_path)

# COMMAND ----------

# DBTITLE 1,Define Inspection Data URL
# Define API URL:
inspection_data_url = "https://data.cityofchicago.org/resource/4ijn-s7e5.csv?$limit=200000000"

# COMMAND ----------

# DBTITLE 1,API Response File Creator
# Define the API response file name
now = datetime.now() # current date and time
fmt_now = now.strftime("%Y%m%d_%H-%M-%S")
	
# Create the empty response file:
try:
  print('---------------------------------------------------')
  print('Creating empty CSV response file.')
  dbutils.fs.put(f"{api_resp_path}/inspections_{fmt_now}.csv", "")
except:
  print('File already exists')

# COMMAND ----------

# DBTITLE 1,Code Snippet 0: API Response File Download
# Call API & populate the response file:
#----------------------------------------------------------
resp = requests.get(inspection_data_url)
if resp.status_code != 200:
    # This means something went wrong
    raise ApiError(f'GET /tasks/ {resp.status_code}')

print("Response Status Code : ", resp.status_code)
resp_csv_str = resp.content.decode("utf-8")
print("Byte size of JSON Response: ", len(resp_csv_str))
#----------------------------------------------------------
  
with open(f"{api_resp_path}/inspections_{fmt_now}.csv","w") as f:
  f.write(resp_csv_str)

# COMMAND ----------

# DBTITLE 1,List Files in Chicago Data Directory
dbutils.fs.ls(api_resp_path)
