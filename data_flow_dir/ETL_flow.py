# Databricks notebook source
import pyspark.sql.types as T

from attributes_dir import attributes as A
from medallion_dir import medallion_factory as MF
from medallion_dir import imedallion as IM

# THIS IS JUST A TEST, CAN I PUSH THIS ???

# COMMAND ----------

medallion = MF.MedallionFactory.create_or_get(
    version = MF.MedallionFactory.Version.V1,
    call = IM.IMedallion.Call.RAW_INTERNAL_DATABASE)

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze - raw data
# MAGIC Reading data from source. Currently just from the internal database but in the future azure data lake with Autoloader

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE default.airbnb_df

# COMMAND ----------

bronze_df = medallion.imedallion_raw_to_bronze_transformation(tbl_nm=A.TableNames.raw_airbnb, fraction=1.0, seed=3)

# COMMAND ----------

bronze_df.count()

# COMMAND ----------

#bronze_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Silver - cleaning data
# MAGIC Removing duplicates and nan

# COMMAND ----------

silver_df = medallion.imedallion_bronze_to_silver_transformation(bronze_df=bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold (curated business-level tables)
# MAGIC Performs aggregation of all the review scores

# COMMAND ----------

gold_df = medallion.imedallion_silver_to_gold_transformation(silver_df=silver_df)

# COMMAND ----------

gold_df.display()

# COMMAND ----------

# gold_df.write.format("delta").mode("overwrite").saveAsTable("default.gold_tbl")
