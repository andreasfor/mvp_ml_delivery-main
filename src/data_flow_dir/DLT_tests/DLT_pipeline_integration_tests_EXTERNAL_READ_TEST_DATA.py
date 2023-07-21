import pyspark.sql.types as T
import pyspark.sql.functions as F

from src.common_dir.common_functions import Common
from src.attributes_dir import attributes as A

# This is only needed for calling spark outside of Databriucks e.g when auto generating documenatation with Sphinx
spark = Common.create_spark_session()

try:
    # Verify that bronze df contains 22 rows
    bronze_df = spark.table("dlt_integration_test_external_read.bronze_dlt_table")
    assert bronze_df.count() == 22
except:
    raise Exception("Integration test fail due to length of bronze dataframe is not as exptected")

try:
    # Verify that silver df contain 2 row after removing duplicates and nan
    silver_df = spark.table("dlt_integration_test_external_read.silver_dlt_table")
    assert silver_df.count() == 2
except:
    raise Exception("Integration test fail due to length of silver dataframe is not as expected after dropping rows with duplicates and nan values")

try:
    # Verify that the aggreagted value from the UDF is 7 for 2 rows
    gold_df = spark.table("dlt_integration_test_external_read.gold_dlt_table")
    assert gold_df.filter(F.col(A.AttributesAdded.aggregated_review_scores.name) == 7).count() == 2
except:
    raise Exception("Integration test fail due to length of gold dataframe is not as expected after aggregating review scores")