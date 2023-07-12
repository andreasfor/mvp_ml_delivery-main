# Databricks notebook source
import attributes

# COMMAND ----------

attributes.AttributesOriginal.accommodates.name == 1

# COMMAND ----------

attributes.AttributesOriginal.accommodates.name

# COMMAND ----------



try:
    if not attributes.AttributesOriginal.accommodates.name == "accommodates":
        assert False
except:
    pass

# COMMAND ----------

attributes.AttributesOriginal.accommodates.name == "egg"

# COMMAND ----------

type(attributes.AttributesOriginal.accommodates.name) == float

# COMMAND ----------

try:
    if not type(attributes.AttributesOriginal.accommodates.name) == float:
        assert False
except:
    

# COMMAND ----------

from attributes_dir import attributes
assert issubclass(attributes.AttributesOriginal, attributes)

# COMMAND ----------

import importlib.util
spam_spec = importlib.util.find_spec("common_dir.common", package="Common")
found = spam_spec is not None

# COMMAND ----------

found

# COMMAND ----------

from common_dir import common
spark = common.Common.create_spark_session()

# COMMAND ----------

spark

# COMMAND ----------

spark3 = SparkSession.builder.getOrCreate
print(spark3)

# COMMAND ----------

type(spark)

# COMMAND ----------

import pyspark.sql as S

# COMMAND ----------

from attributes_dir import attributes as A
daily_df = spark.table(A.TableNames.test_df_simulate_daily_inserts)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Create DLT unit test dataset

# COMMAND ----------

from attributes_dir import attributes as A
from pyspark.sql import Row
from random import randint, uniform
import pyspark.sql.types as T
import pyspark.sql.functions as F

# COMMAND ----------

# This dataset will be used to verify the DLT expectations in the bronze layer and the dropna in the silver layer
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

# COMMAND ----------

#Replace empty string with None value for attribute cancellation_policy
adding_none_to_random_df = random_df.withColumn(A.AttributesOriginal.cancellation_policy.name, \
       F.when(F.col(A.AttributesOriginal.cancellation_policy.name)==" " ,None) \
          .otherwise(F.col(A.AttributesOriginal.cancellation_policy.name)))

# COMMAND ----------

# This row is used for verifying the aggregation UDF in the gold layer
random_data_2 = []
for _ in range(1):
    random_row_2 = Row(
        host_is_superhost=str(randint(0, 1)),
        cancellation_policy="Random",
        instant_bookable=str(randint(0, 1)),
        host_total_listings_count=uniform(1, 10),
        neighbourhood_cleansed="Random",
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
        review_scores_rating=1,
        review_scores_accuracy=1,
        review_scores_cleanliness=1,
        review_scores_checkin=1,
        review_scores_communication=1,
        review_scores_location=1,
        review_scores_value=1,
        price=uniform(10, 100)
    )
    random_data_2.append(random_row_2)

    random_2_df = spark.createDataFrame(random_data_2)

# COMMAND ----------

# Union of the two random dataframes
combined_random_df = random_df.union(random_2_df)

# COMMAND ----------

combined_random_df.display()

# COMMAND ----------

type(combined_random_df)

# COMMAND ----------

combined_random_df.write.format("delta").mode("overwrite").saveAsTable("default.test_dlt_combined_random_df")

# COMMAND ----------

read_back_combined_random_df = spark.table("default.test_dlt_combined_random_df")

# COMMAND ----------

read_back_combined_random_df.count()

# COMMAND ----------

read_back_combined_random_df.display()

# COMMAND ----------

drop_df = read_back_combined_random_df.dropDuplicates().dropna()

# COMMAND ----------

drop_df.display()

# COMMAND ----------

read_back_combined_random_df.dropDuplicates().dropna().count()

# COMMAND ----------

import subprocess
import sys

# Need to call subbprocess in order to call shell scripts in DLT 
subprocess.check_call([sys.executable, "-m", "pip", "install", "dlt-with-debug"])

# COMMAND ----------

# MAGIC %pip install -e git+https://github.com/souvik-databricks/dlt-with-debug.git#"egg=dlt_with_debug"

# COMMAND ----------

silver_df = read_back_combined_random_df.dropDuplicates().dropna()

silver_df.display()

# COMMAND ----------

from attributes_dir import attributes as A
import pyspark.sql.functions as F

# COMMAND ----------

# filter out rows with blank strings in all the columns
assert silver_df.filter(F.col(A.AttributesOriginal.review_scores_value.name) == 1).count() == 2

# COMMAND ----------

def func_1():
    print("inside func 1")

# COMMAND ----------

def func_2():
    func_1()
    print("inside func 2")

# COMMAND ----------

func_2()

# COMMAND ----------

from data_flow_dir import help_function_dlt
help_function_dlt.DLT_Helper.random_df_str_nm_fn()

# COMMAND ----------

gold_df = spark.table("dlt_integration_test.gold_dlt_table")

# COMMAND ----------

gold_df.display()

# COMMAND ----------

gold_df.printSchema()

# COMMAND ----------

@dlt.expect_all_or_fail({"host_is_superhost data values": "host_is_superhost in ('0', '1')",
                         "host_is_superhost is not null": "host_is_superhost IS NOT NULL",
                         host_is_superhost empty string no space", "host_is_superhost != '',
                         host_is_superhost empty string space", "host_is_superhost != ' ' 
                         })
@dlt.expect_or_fail("cancellation_policy_not_null", "cancellation_policy IS NOT NULL")
@dlt.expect_or_fail("cancellation_policy_empty_str", "cancellation_policy != ' ' ")
@dlt.expect_or_fail("neighbourhood_cleansed_not_null", "neighbourhood_cleansed IS NOT NULL")
@dlt.expect_or_fail("neighbourhood_cleansed_empty_str", "neighbourhood_cleansed != ' '")

# COMMAND ----------

{"host_is_superhost is str": "host_is_superhost IS NOT NULL",
"type is not null": "type is not null"}
