# Databricks notebook source
# This cell is used to run the transformation between pandas and pyspark
# This cell is necessary due to if cluster runtime version is under 13, I think
# %pip install -U pandas==1.5.3

# COMMAND ----------

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import Row
import pyspark.sql.types as T
import pyspark.sql.functions as F

from scipy.stats import boxcox, johnsonsu
from random import randint, uniform
import numpy as np

from src.attributes_dir import attributes as A

# This is only needed for calling spark outside of Databriucks e.g when auto generating documenatation with Sphinx
sc = SparkContext('local')
spark = SparkSession(sc)

# COMMAND ----------

airbnb_df =  spark.table(A.TableNames.raw_airbnb)
# airbnb_df = airbnb_df.sample(fraction=0.1, seed=3)

# COMMAND ----------

train_df, val_df, test_df = airbnb_df.randomSplit([.6, .2, .2], seed=42)

# COMMAND ----------

# Save data to be compared to daily distribution in order to measure the data drift
# this train data is based on spark.table(A.TableNames.gold_tbl) i.e. the table that the model was trained on 
# train_df.write.format("delta").mode("overwrite").saveAsTable(A.TableNames.reference_data_data_drift_train_data_only)

# COMMAND ----------

# Code in this cell is generated with chatGPT
# Generate random data for the new rows that the pipeline has not been trained on
# This will trigger exceptions
random_data = []
for _ in range(10):
    random_row = Row(
        host_is_superhost=str(randint(0, 1)),
        cancellation_policy="Random Policy",
        instant_bookable=str(randint(0, 1)),
        host_total_listings_count=uniform(1, 10),
        neighbourhood_cleansed="Random Neighbourhood",
        latitude=uniform(-90, 90),
        longitude=uniform(-180, 180),
        property_type="Random Property Type",
        room_type="Random Room Type",
        accommodates=uniform(1, 10),
        bathrooms=uniform(1, 5),
        bedrooms=uniform(1, 5),
        beds=uniform(1, 5),
        bed_type="Random Bed Type",
        minimum_nights=randint(1, 10),
        number_of_reviews=randint(0, 100),
        review_scores_rating=uniform(0, 100),
        review_scores_accuracy=uniform(0, 10),
        review_scores_cleanliness=uniform(0, 10),
        review_scores_checkin=uniform(0, 10),
        review_scores_communication=uniform(0, 10),
        review_scores_location=uniform(0, 10),
        review_scores_value=uniform(0, 10),
        price=uniform(10, 100)
    )
    random_data.append(random_row)

# Convert the random data to a DataFrame
random_df = spark.createDataFrame(random_data)

# Union the random DataFrame with the original DataFrame
new_data_combined_df = test_df.union(random_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add none values
# MAGIC These values are supposed to be visualized but let through in the DLT pipeline since we are only using the expectation of warning. the two features that we expect to not be null is neighbourhood_cleansed and cancellation_policy. cancellation_policy will contain None values and neighbourhood_cleansed empty strings.

# COMMAND ----------

random_data = []
for _ in range(10):
    random_row = Row(
        host_is_superhost=str(randint(0, 1)),
        cancellation_policy=" ",
        instant_bookable=str(randint(0, 1)),
        host_total_listings_count=uniform(1, 10),
        neighbourhood_cleansed=" ",
        latitude=uniform(-90, 90),
        longitude=uniform(-180, 180),
        property_type="Random Property Type",
        room_type="Random Room Type",
        accommodates=uniform(1, 10),
        bathrooms=uniform(1, 5),
        bedrooms=uniform(1, 5),
        beds=uniform(1, 5),
        bed_type="Random Bed Type",
        minimum_nights=randint(1, 10),
        number_of_reviews=randint(0, 100),
        review_scores_rating=uniform(0, 100),
        review_scores_accuracy=uniform(0, 10),
        review_scores_cleanliness=uniform(0, 10),
        review_scores_checkin=uniform(0, 10),
        review_scores_communication=uniform(0, 10),
        review_scores_location=uniform(0, 10),
        review_scores_value=uniform(0, 10),
        price=uniform(10, 100)
    )
    random_data.append(random_row)

# Convert the random data to a DataFrame
random_df = spark.createDataFrame(random_data)

# Union the random DataFrame with the original DataFrame
adding_none_to_new_data_combined_df = new_data_combined_df.union(random_df)

# COMMAND ----------

#Replace empty string with None value for attribute cancellation_policy
adding_none_to_new_data_combined_df = adding_none_to_new_data_combined_df.withColumn(A.AttributesOriginal.cancellation_policy.name, \
       F.when(F.col(A.AttributesOriginal.cancellation_policy.name)==" " ,None) \
          .otherwise(F.col(A.AttributesOriginal.cancellation_policy.name)))

# COMMAND ----------

adding_none_to_new_data_combined_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE default.test_df_simulate_daily_inserts

# COMMAND ----------

# Save test_df and use it to simuluate daily inserts
# Dropping price since that is the target variable
# adding_none_to_new_data_combined_df.drop("price").write.format("delta").mode("overwrite").saveAsTable(A.TableNames.test_df_simulate_daily_inserts)

# COMMAND ----------

# MAGIC %md
# MAGIC # Skew the data
# MAGIC The skewed data will be used to trigger data drift and model drift 

# COMMAND ----------

daily_df = spark.table("dlt.gold_dlt_table") # This data has been cleaned and aggregated
# daily_df = spark.table(A.TableNames.test_df_simulate_daily_inserts) # This data has NOT been cleaned and aggregated

# COMMAND ----------

daily_pdf = daily_df.toPandas()

# COMMAND ----------

daily_pdf.head()

# COMMAND ----------

# Define the columns you want to skew
columns_to_skew = [col.name for col in daily_df.schema.fields if col.dataType == T.DoubleType()]

# Code from chatGPT
# Iterate over the columns
for column in columns_to_skew:
    # Extract the column data as a 1-dimensional array
    data_array = daily_pdf[column].to_numpy()

    # Calculate parameters for the Johnson distribution using moments
    param = johnsonsu.fit(data_array)

    # Generate skewed data based on the estimated parameters
    skewed_data = johnsonsu.rvs(*param, size=len(data_array))

    # Replace the original column with the skewed data
    daily_pdf[column] = skewed_data

# COMMAND ----------

daily_pdf.head()

# COMMAND ----------

# If below cell crash, try this row to enable pdf to df
# spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# COMMAND ----------

#Create PySpark DataFrame from Pandas
skewed_daily_df=dataframe = spark.createDataFrame(daily_pdf)

# COMMAND ----------

skewed_daily_df.display()

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- DROP TABLE default.skewed_test_df_simulate_daily_inserts

# COMMAND ----------

# skewed_daily_df.write.format("delta").mode("overwrite").saveAsTable(A.TableNames.skewed_test_df_simulate_daily_inserts)
