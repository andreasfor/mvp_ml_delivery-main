import pyspark
import pyspark.sql.types as T
import pyspark.sql.functions as F
from functools import reduce

from src.attributes_dir import attributes as A
from src.common_dir.common_functions import Common
import src.medallion_dir.medallion_factory as MF
import src.medallion_dir.imedallion as IM
import src.medallion_dir.support_functions as SF

import dlt

# This is only needed for calling spark outside of Databriucks e.g when auto generating documenatation with Sphinx
spark = Common.create_spark_session()

# I have not succeded to put this function outside of this file. It seems to be a problem when calling a class/function and use it in a UDF when using DLT
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

    aggregated_value = float(review_scores_rating) + float(review_scores_accuracy) + float(review_scores_cleanliness) + float(review_scores_checkin) + float(review_scores_communication) + float(review_scores_location) + float(review_scores_value)

    return float(aggregated_value)

_aggregate_reviews_udf = spark.udf.register("_aggregate_reviews", _aggregate_reviews, returnType=T.DoubleType())

@dlt.table(name="bronze_dlt_table", comment="Reading data from the internal database. Expect cancellation_policy and neighbourhood_cleansed to not be null. However, we are letting the data pass (since we use expect and not expect_or_drop/fail) but we are recording the data quality. The selected features are the two most important features according to the feature importance plot which is produced when training the Random Forest model for this particular problem. In addition, the current data pipeline, see silver transformation, will skip rows with null values anyway but the reasoning holds for what features that should not be null. Read more concerning expections at https://docs.databricks.com/delta-live-tables/expectations.html")
@dlt.expect("cancellation_policy_not_null", "cancellation_policy IS NOT NULL")
@dlt.expect("cancellation_policy_empty_str", "cancellation_policy != ' ' ")
@dlt.expect("neighbourhood_cleansed_not_null", "neighbourhood_cleansed IS NOT NULL")
@dlt.expect("neighbourhood_cleansed_empty_str", "neighbourhood_cleansed != ' '")
def medallion_read_adls_merge_raw_into_bronze_transformation() -> pyspark.sql.dataframe.DataFrame:

    """This function reads the raw data from source. This version reads from internal database and returns a bronze dataframe.

    :return: pyspark.sql.dataframe.DataFrame

    """
    # The version of data to be runed is controlled via the Configuration in pipeline settings of the DLT
    # We have the possibility run the test data or the read data, i.e. airbnb_1
    # We can also run a dlt test which calls a mock dataset which wconsists of 11 rows and will trigger additional tests for the dlt pipeline 
    
    mnt_path = spark.conf.get("mnt_path")
    test_mode = spark.conf.get("test_mode")

    #mnt_path = "dbfs:/mnt/azure_data_lake/airbnb/airbnb_1.csv"
    #test_mode = "true"

    # One can not set a boolan value as a task value in jobs, only strings, hence converting it here
    if test_mode == "true":
        test_mode = True
    else:
        test_mode = False

    medallion = MF.MedallionFactory.create_or_get(
                version = MF.MedallionFactory.Version.V1,
                call = IM.IMedallion.Call.RAW_INTERNAL_DATABASE)

    bronze_df = medallion.imedallion_read_adls_merge_raw_into_bronze_transformation(mnt_path=mnt_path, test_mode=test_mode)

    return bronze_df


@dlt.table(name="silver_dlt_table", comment="Cleaning data by dropping duplicates and nan values")
def medallion_bronze_to_silver_dlt_transformation() -> pyspark.sql.dataframe.DataFrame:
    
    """
    This function reads cleans the bronze dataframe by removing duplicates and removing nan and returns a silver dataframe.

    :return: pyspark.sql.dataframe.DataFrame
    """
     
    silver_df = dlt.read("bronze_dlt_table").dropDuplicates().dropna()

    categorical_cols = [field for (field, dataType) in silver_df.dtypes if dataType == "string"]

    # filter out rows with blank strings in all the columns
    no_space_silver_df = silver_df.filter(reduce(lambda x, y: x & y, [(F.col(c) != ' ') & (F.col(c) != '') for c in categorical_cols]))

    return no_space_silver_df

@dlt.table(name="gold_dlt_table", comment="Aggregates review scores")
def medallion_silver_to_gold_dlt_transformation() -> pyspark.sql.dataframe.DataFrame:

    """This function aggregates review scores of the silver dataframe and returns a gold dataframe.

    :return: pyspark.sql.dataframe.DataFrame
    """

    gold_df = dlt.read("silver_dlt_table").withColumn(A.AttributesAdded.aggregated_review_scores.name, _aggregate_reviews_udf(A.AttributesOriginal.review_scores_rating.name, A.AttributesOriginal.review_scores_accuracy.name, A.AttributesOriginal.review_scores_cleanliness.name, A.AttributesOriginal.review_scores_checkin.name, A.AttributesOriginal.review_scores_communication.name, A.AttributesOriginal.review_scores_location.name, A.AttributesOriginal.review_scores_value.name))

    return gold_df
