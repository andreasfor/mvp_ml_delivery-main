# Databricks notebook source
from attributes_dir import attributes as A
import dlt

# I have not succeded to put this function outside of this file. It seems to be a problem when calling a class/function and use it in a udf when using DLT
def _aggregate_reviews(review_scores_accuracy, review_scores_cleanliness, review_scores_checkin, review_scores_communication, review_scores_location, review_scores_value):

    aggregated_value = review_scores_accuracy + review_scores_cleanliness + review_scores_checkin + review_scores_communication + review_scores_location + review_scores_value

    return aggregated_value

_aggregate_reviews_udf = spark.udf.register("_aggregate_reviews", _aggregate_reviews)

# COMMAND ----------

@dlt.table(name="bronze_dlt_table",
comment="Reading data from the internal database. Expect cancellation_policy and neighbourhood_cleansed to not be null. Hpowever, we are letting the data pass (since we use expect and not expect_or_drop/fail) but we are recording the data quality. The selected features are the two most important features according to the feature importance plot which is produced when training the Random Forest model for this particular problem. However, the current data pipeline, see silver transformation, will skip rows with null values anyway but the reasoning holds for what features that should not be null. Read more concerning expections at https://docs.databricks.com/delta-live-tables/expectations.html")
@dlt.expect("cancellation_policy_not_null", "cancellation_policy IS NOT NULL")
@dlt.expect("cancellation_policy_empty_str", "cancellation_policy != ' ' ")
@dlt.expect("neighbourhood_cleansed_not_null", "neighbourhood_cleansed IS NOT NULL")
@dlt.expect("neighbourhood_cleansed_empty_str", "neighbourhood_cleansed != ' '")
def medallion_raw_to_bronze_dlt_transformation():

    """This function reads the raw data from source. This version reads from internal database and returns a bronze dataframe.

    :return: S.dataframe.DataFrame

    :raise: ?: ?
    
    """
    # The version of data to be runed is controlled via the Configuration in pipeline settings of the DLT
    # We have the possibility run the test data or the skewed test data  
    run_data_version = spark.conf.get("run_data_version")
    print("run_data_version", run_data_version)
    bronze_df = spark.table(run_data_version)

    bronze_df.count()

    return bronze_df

# COMMAND ----------

@dlt.table(name="silver_dlt_table",
comment="Cleaning data by dropping duplicates and nan values")
def medallion_bronze_to_silver_dlt_transformation():
    
    """This function reads cleans the bronze dataframe by removing duplicates and removing nan and returns a silver dataframe.

    :return: S.dataframe.DataFrame

    :raise: ?: ?"""


    silver_df = dlt.read("bronze_dlt_table").dropDuplicates().dropna()

    return silver_df

# COMMAND ----------

@dlt.table(name="gold_dlt_table",
comment="Aggregates review scores")
def medallion_silver_to_gold_dlt_transformation():

    """This function aggregates review scores of the silver dataframe and returns a gold dataframe.

    :return: S.dataframe.DataFrame

    :raise: ?: ?"""

    gold_df = dlt.read("silver_dlt_table").withColumn(A.AttributesAdded.aggregated_review_scores.name, _aggregate_reviews_udf(A.AttributesOriginal.review_scores_accuracy.name, A.AttributesOriginal.review_scores_cleanliness.name, A.AttributesOriginal.review_scores_checkin.name, A.AttributesOriginal.review_scores_communication.name, A.AttributesOriginal.review_scores_location.name, A.AttributesOriginal.review_scores_value.name))

    '''
    # Change order of columns and put target last
    cols = gold_df.columns

    ordered_gold_df = gold_df.select([*cols])
    
    # Removing target feature if it is present
    if A.AttributesTarget.price.name in cols:
        cols.remove(A.AttributesTarget.price.name)

        ordered_gold_df = gold_df.select([*cols, A.AttributesTarget.price.name])
    else:
        ordered_gold_df = gold_df.select([*cols])'''

    return gold_df
