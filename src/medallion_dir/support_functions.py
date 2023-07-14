#import os
#import sys
#sys.path.append(os.path.abspath('/Workspace/Repos/andreas.forsberg@capgemini.com/mvp_ml_delivery'))

import pyspark
import pyspark.sql.types as T
import pyspark.sql.functions as F

from pyspark.sql import Row
from random import randint, uniform

from src.attributes_dir import attributes as A
from src.common_dir.common_functions import Common


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

def create_mock_dataset() -> str:
    """This function creates a mock dataset which will be used for testing the medallion component. The function creates the dataset and saves it as a table. 
    
    :return: The name of the table created
    :rtype: str
    """
    
    spark = Common.create_spark_session()

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

    #Replace empty string with None value for attribute cancellation_policy
    adding_none_to_random_df = random_df.withColumn(A.AttributesOriginal.cancellation_policy.name, \
        F.when(F.col(A.AttributesOriginal.cancellation_policy.name)==" " ,None) \
            .otherwise(F.col(A.AttributesOriginal.cancellation_policy.name)))
    
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

        # Union of the two random dataframes
        combined_random_df = adding_none_to_random_df.union(random_2_df)

        tbl_nm = "default.test_medallion_combined_random_df"

        combined_random_df.write.format("delta").mode("overwrite").saveAsTable(tbl_nm)

        return tbl_nm