"""
This test is for a Databricks function called Delta Live Tables. These are a bit complicated at a first glance (but after a few experiments you will be fine) and so are the testing of them. Therefore, is there a few work arounds in this notebook.  
1, You cannot run a DLT as a normal notebook. It needs to be run via workflows, which is a bit tedious when experimenting. There is a workaround for this problem; see package dlt_with_debug at link https://github.com/souvik-databricks/dlt-with-debug .
2, You cannot run pip install without calling subprocess.
3, You cannot run dlt-with-debug with table names, e.g. @dlt.create_table(name="bronze_dlt_table", ... ). It is therefore commented out. 
"""
import subprocess
import sys
import os

# Need to call subbprocess in order to call shell scripts in DLT 
subprocess.check_call([sys.executable, "-m", "pip", "install", "pytest"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "dlt-with-debug"])

import pytest
from dlt_with_debug import dltwithdebug, pipeline_id, showoutput

if pipeline_id:
  import dlt
else:
  from dlt_with_debug import dlt

from random import randint, uniform
import pyspark.sql.types as T
import pyspark.sql.functions as F
import pyspark.sql as S

# This row allows importing modules from folders
sys.path.append(os.path.abspath('/Workspace/Repos/andreas.forsberg@capgemini.com/mvp_ml_delivery'))

from attributes_dir import attributes as A
from common_dir import common

import pyspark
# import dlt


#This is only needed for calling spark outside of Databriucks e.g when auto generating documenatation with Sphinx
spark = common.Common.create_spark_session()

# I did not manage to run this helper function as a pytest fixture when running DLT job
def random_df_str_nm_fn() -> str:
    """ 
    :return: A table name wich relates to the test dataframe created for each pytest session
    :rtype: pyspark.sql.dataframe.DataFrame
    """

    # This dataset will be used to verify the DLT expectations in the bronze layer and the dropna in the silver layer
    random_data = []
    for _ in range(10):
        random_row = S.Row(
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

    random_df = spark.createDataFrame(random_data)

    #Replace empty string with None value for attribute cancellation_policy
    adding_none_to_random_df = random_df.withColumn(A.AttributesOriginal.cancellation_policy.name, \
        F.when(F.col(A.AttributesOriginal.cancellation_policy.name)==" " ,None) \
            .otherwise(F.col(A.AttributesOriginal.cancellation_policy.name)))

    
    # This row is used for verifying the aggregation UDF in the gold layer
    random_data_2 = []
    for _ in range(1):
        random_row_2 = S.Row(
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

        # Union of the two random dataframes
        combined_random_df = adding_none_to_random_df.union(random_2_df)

        combined_random_df_str_nm = "default.test_dlt_combined_random_df"

        combined_random_df.write.format("delta").mode("overwrite").saveAsTable(combined_random_df_str_nm)

    return combined_random_df_str_nm
    
    
# I have not succeded to put this function outside of this file. It seems to be a problem when calling a class/function and use it in a udf when using DLT
def _aggregate_reviews(review_scores_rating, review_scores_accuracy, review_scores_cleanliness, review_scores_checkin, review_scores_communication, review_scores_location, review_scores_value) -> float:
    """
    Aggregate all the review scores into one number. This function shows how to create an UDF and use it in a Delta Live Table workflow.

    :param review_scores_rating: How accurate the reviews are. From 1-10.
    :type review_scores_rating: double
    :param review_scores_accuracy: How accurate the reviews are. From 1-10.
    :type review_scores_accuracy: double 
    :param review_scores_cleanliness: How clean the rental was. From 1-10
    :param review_scores_checkin: How easy the check in was. From 1-10.
    :type review_scores_checkin: double 
    :param review_scores_communication: How well the communication went. From 1-10.
    :type review_scores_communication: double
    :param review_scores_location: Was the rental located in a good area. From 1-10.
    :type review_scores_location: double
    :param review_scores_value: Was the price applicable for the rental. From 1-10.
    :type review_scores_value: double
    :return: The aggregated number of all the reviews.
    :rtype: float
    """

    aggregated_value = review_scores_rating + review_scores_accuracy + review_scores_cleanliness + review_scores_checkin + review_scores_communication + review_scores_location + review_scores_value

    return float(aggregated_value)

_aggregate_reviews_udf = spark.udf.register("_aggregate_reviews", _aggregate_reviews, returnType=T.DoubleType())

#@dlt.create_table(name="bronze_dlt_table", comment="Reading data from the internal database. Expect cancellation_policy and neighbourhood_cleansed to not be null. Hpowever, we are letting the data pass (since we use expect and not expect_or_drop/fail) but we are recording the data quality. The selected features are the two most important features according to the feature importance plot which is produced when training the Random Forest model for this particular problem. However, the current data pipeline, see silver transformation, will skip rows with null values anyway but the reasoning holds for what features that should not be null. Read more concerning expections at https://docs.databricks.com/delta-live-tables/expectations.html")
@dlt.create_table()
#@dlt.expect("cancellation_policy_not_null", "cancellation_policy IS NOT NULL")
#@dlt.expect("cancellation_policy_empty_str", "cancellation_policy != ' ' ")
#@dlt.expect("neighbourhood_cleansed_not_null", "neighbourhood_cleansed IS NOT NULL")
#@dlt.expect("neighbourhood_cleansed_empty_str", "neighbourhood_cleansed != ' '")
@dltwithdebug(globals())
def medallion_raw_to_bronze_dlt_transformation() -> pyspark.sql.dataframe.DataFrame:

    """This function reads the raw data from source. This version reads from internal database and returns a bronze dataframe.

    :return: pyspark.sql.dataframe.DataFrame

    """
    # The version of data to be runed is controlled via the Configuration in pipeline settings of the DLT
    # We have the possibility run the test data or the skewed test data
    # The run_data_version is not possible in the test, here will we use the mock dataset random_df
    # run_data_version = spark.conf.get("run_data_version")

    rand_df_str_nm = random_df_str_nm_fn()

    bronze_df = spark.table(rand_df_str_nm)

    bronze_df.count()

    # Verify that df contains 11 rows
    assert bronze_df.count() == 11

    return bronze_df

#@dlt.create_table(name="silver_dlt_table", comment="Cleaning data by dropping duplicates and nan values")
@dlt.create_table()
@dltwithdebug(globals())
def medallion_bronze_to_silver_dlt_transformation() -> pyspark.sql.dataframe.DataFrame:
    
    """
    This function reads cleans the bronze dataframe by removing duplicates and removing nan and returns a silver dataframe.

    :return: pyspark.sql.dataframe.DataFrame
    """

    read_bronze_df = dlt.read("medallion_raw_to_bronze_dlt_transformation")

    silver_df = read_bronze_df.dropDuplicates().dropna()

    cnt_silver_df = silver_df.count()

    print("cnt_silver_df", cnt_silver_df)

    # silver_df = dlt.read("medallion_raw_to_bronze_dlt_transformation").dropDuplicates().dropna()

    # Verify that df contains 1 row after dropping duplicates
    assert cnt_silver_df == 1

    return silver_df

#@dlt.create_table(name="gold_dlt_table", comment="Aggregates review scores")
@dlt.create_table()
@dltwithdebug(globals())
def medallion_silver_to_gold_dlt_transformation() -> pyspark.sql.dataframe.DataFrame:

    """This function aggregates review scores of the silver dataframe and returns a gold dataframe.

    :return: pyspark.sql.dataframe.DataFrame
    """

    gold_df = dlt.read("medallion_bronze_to_silver_dlt_transformation").withColumn(A.AttributesAdded.aggregated_review_scores.name, _aggregate_reviews_udf(A.AttributesOriginal.review_scores_rating.name, A.AttributesOriginal.review_scores_accuracy.name, A.AttributesOriginal.review_scores_cleanliness.name, A.AttributesOriginal.review_scores_checkin.name, A.AttributesOriginal.review_scores_communication.name, A.AttributesOriginal.review_scores_location.name, A.AttributesOriginal.review_scores_value.name))
    
    # Verify that the aggreagted value from the UDF is 7
    assert gold_df.filter(F.col(A.AttributesAdded.aggregated_review_scores.name) == 7)

    return gold_df


# See the output
showoutput(medallion_bronze_to_silver_dlt_transformation)
