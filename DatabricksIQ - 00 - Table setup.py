# Databricks notebook source
# DBTITLE 1,Table of Movie Budgets Extraction
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.imdb.budgets AS (
# MAGIC   SELECT
# MAGIC     title,
# MAGIC     REGEXP_SUBSTR(production_companies, '^[^-]+') AS production_company,
# MAGIC     release_date,
# MAGIC     week,
# MAGIC     budget,
# MAGIC     overview,
# MAGIC     popularity,
# MAGIC     vote_average,
# MAGIC     vote_count
# MAGIC   FROM
# MAGIC     main.np_onboarding.movies_silver
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Enriched Movies View
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW main.imdb.movies_enriched_view AS
# MAGIC SELECT m.*, b.budget, b.week, b.production_company, b.overview, b.vote_average
# MAGIC FROM main.imdb.movies AS m
# MAGIC JOIN main.imdb.budgets AS b ON m.title = b.title;
# MAGIC
# MAGIC SELECT * FROM main.imdb.movies_enriched_view;
