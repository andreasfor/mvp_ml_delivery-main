# Databricks notebook source
# Make widget, these can be set in the job and then extracted here 
dbutils.widgets.text(name="run_model_version", defaultValue="Empty")
run_model_version = str(getArgument("run_model_version"))

# COMMAND ----------

run_model_version

# COMMAND ----------

# run_model_version = "runs:/a33aac06825542f58a1bef6c86468263/trained_pipeline"

# COMMAND ----------

import mlflow
import evidently # Be aware that Evidently has been installed both on the all purpuse cluster and on the job cluster via PyPi in order to run this notebook in a Job and without using %pip install in notebook
from evidently.report import Report 
from evidently.metric_preset import DataDriftPreset
import delta.tables as DT
import pyspark.sql.types as T
import pyspark.sql.functions as F

from datetime import date

from attributes_dir import attributes as A
import inference_support as IS

# COMMAND ----------

inference_support = IS.InferenceSupportClass()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE default.unseen_data_passed_to_model

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE dlt.silver_dlt_table

# COMMAND ----------

# Reading data from the previous task, i.e. the DLT pipeline 
daily_df = spark.table("dlt.gold_dlt_table")

reference_data_data_drift_df = spark.table(A.TableNames.reference_data_data_drift_train_data_only)

# COMMAND ----------

daily_df.display()

# COMMAND ----------

# DBTITLE 1,Load pipeline and predict
logged_pipeline = run_model_version

loaded_pipeline = mlflow.spark.load_model(logged_pipeline)

daily_pred_df = loaded_pipeline.transform(daily_df)

# COMMAND ----------

daily_pred_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE default.daily_pred_df

# COMMAND ----------

daily_pred_df = daily_pred_df.withColumnRenamed(existing="prediction", new="price")
daily_pred_df.write.format("delta").mode("overwrite").saveAsTable(A.TableNames.daily_pred_df)

# COMMAND ----------

daily_pred_df.display()

# COMMAND ----------

inference_support.check_if_unseeen_data_is_passed_to_model_fn(daily_df, daily_pred_df)

# COMMAND ----------

# Unseen data is data that e.g. the pipeline has not seen before. Such as a certain cancelation policy. Therefore can't e.g. the string indexer map it. Comes from the function _check_if_unseeen_data_is_passed_to_model_fn
# # This data should be saved in the dashboard so we can track what data has not been predicted
# unseen_data_df = spark.table(A.TableNames.unseen_data_passed_to_model)

# COMMAND ----------

#unseen_data_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Measure datadrift and target column drift
# MAGIC
# MAGIC Note that evidently is installed on the cluster to avoid the need for pip install every time this notebook is called.
# MAGIC
# MAGIC - Read https://github.com/evidentlyai/evidently 
# MAGIC - Read https://www.evidentlyai.com/

# COMMAND ----------

# MAGIC %md
# MAGIC ## Datadrift incomming data
# MAGIC The data_drift_fn will drop price due to that will not be part of the incomming data. 
# MAGIC Price will be measured in the target drift

# COMMAND ----------

# Uncomment to use skewed data
# daily_df = spark.table("default.skewed_test_df_simulate_daily_inserts_cleaned")

# COMMAND ----------

inference_support.data_drift_fn(daily_df, reference_data_data_drift_df)

# COMMAND ----------

with open(f"/dbfs/FileStore/data_drift_report/{date.today()}.html", "r") as f:
  data = "".join([l for l in f])

displayHTML(data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model drift
# MAGIC Check if the predicted values are of the same distribution as the training column "price".
# MAGIC Note that we need to change the name of prediction to have the same name as the original column

# COMMAND ----------

daily_pred_df.display()

# COMMAND ----------

reference_data_data_drift_df.display()

# COMMAND ----------

inference_support.model_drift_fn(daily_pred_df, reference_data_data_drift_df)

# COMMAND ----------

with open(f"/dbfs/FileStore/model_drift_report/{date.today()}.html", "r") as f:
  data = "".join([l for l in f])

displayHTML(data)

# COMMAND ----------

# MAGIC %md
# MAGIC # Create graphs that will be on the dashboard
# MAGIC # REMOVE THIS WHEN COMFIMRED THE SQL DASHBOARD IS WORKING

# COMMAND ----------

daily_pred_df.display()
