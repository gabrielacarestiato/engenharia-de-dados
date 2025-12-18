# Databricks notebook source
# MAGIC %sql
# MAGIC DROP CATALOG IF EXISTS mvp CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG mvp

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG mvp

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS staging CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA staging

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS bronze CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS silver CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA silver

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS gold CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA gold

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA staging

# COMMAND ----------

spark.sql("USE CATALOG mvp")
spark.sql("USE SCHEMA staging")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS players
