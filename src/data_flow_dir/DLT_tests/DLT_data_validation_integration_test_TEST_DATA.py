"""
This file is only meant to be used for dlt_integration_test and not the daily pipeline.
"""

import sys
import os

# This row allows importing modules from folders
sys.path.append(os.path.abspath('/Workspace/Repos/andreas.forsberg@capgemini.com/mvp_ml_delivery'))

from attributes_dir import attributes as A
from common_dir import common

import pyspark
import dlt

# This is only needed for calling spark outside of Databriucks e.g when auto generating documenatation with Sphinx
spark = common.Common.create_spark_session()

@dlt.create_table(name="data_validation_gold_layer_dlt_integration_test", comment="Validates the data of the golding layer with expectations set to fail")
@dlt.expect_all_or_fail({"host_is_superhost is not null": "host_is_superhost IS NOT NULL",
                         "host_is_superhost empty string no space": "host_is_superhost != ''",
                         "host_is_superhost empty string space": "host_is_superhost != ' '" 
                         })
@dlt.expect_or_fail("cancellation_policy_not_null", "cancellation_policy IS NOT NULL")
@dlt.expect_or_fail("cancellation_policy_empty_str", "cancellation_policy != ' ' ")
@dlt.expect_or_fail("neighbourhood_cleansed_not_null", "neighbourhood_cleansed IS NOT NULL")
@dlt.expect_or_fail("neighbourhood_cleansed_empty_str", "neighbourhood_cleansed != ' '")
def data_validation_gold_layer_integration_test() -> pyspark.sql.dataframe.DataFrame:

    """Validates the data of the golding layer with expectations set to fail. The expectations for this function is showing two examples. One example is using expect_all_or_fail and takes a dictionary as a parameter. 
    The second example shows expect_or_fail which only takes a key value pair.
    The examples are just showing how one can use these techniques for three features only.

    :return: pyspark.sql.dataframe.DataFrame
    """

    gold_df = spark.table("dlt_integration_test.gold_dlt_table")

    return gold_df
