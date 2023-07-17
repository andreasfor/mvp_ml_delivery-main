"""
This notebook is for developing tests for a Databricks function called Delta Live Tables. These are a bit complicated at a first glance (but after a few experiments you will be fine) and so are the testing of them. Therefore, is there a few work arounds in this notebook.  
1, You cannot run a DLT as a normal notebook. It needs to be run via workflows, which is a bit tedious when experimenting. There is a workaround for this problem; see package dlt_with_debug at link https://github.com/souvik-databricks/dlt-with-debug .
2, You cannot run pip install without calling subprocess.
3, You cannot run dlt-with-debug with table names, e.g. @dlt.create_table(name="bronze_dlt_table", ... ). It is therefore commented out. 
"""

import pyspark
import subprocess
import sys
import os

# Need to call subbprocess in order to call shell scripts in DLT 
subprocess.check_call([sys.executable, "-m", "pip", "install", "dlt-with-debug"])

from dlt_with_debug import dltwithdebug, pipeline_id, showoutput

if pipeline_id:
  import dlt
else:
  from dlt_with_debug import dlt

from random import randint, uniform
import pyspark.sql.types as T
import pyspark.sql.functions as F
import pyspark.sql as S

from src.common_dir.common_functions import Common
from src.attributes_dir import attributes as A
from src.data_flow_dir.DLT_tests.help_function_dlt import DLT_Helper


#This is only needed for calling spark outside of Databriucks e.g when auto generating documenatation with Sphinx
spark = Common.create_spark_session()

#@dlt.create_table(name="bronze_dlt_table", comment="Reading data from the internal database. Expect cancellation_policy and neighbourhood_cleansed to not be null. Hpowever, we are letting the data pass (since we use expect and not expect_or_drop/fail) but we are recording the data quality. The selected features are the two most important features according to the feature importance plot which is produced when training the Random Forest model for this particular problem. However, the current data pipeline, see silver transformation, will skip rows with null values anyway but the reasoning holds for what features that should not be null. Read more concerning expections at https://docs.databricks.com/delta-live-tables/expectations.html")
@dlt.create_table()
@dlt.expect_all_or_fail({"host_is_superhost data values": "host_is_superhost in ('0', '1')",
                         "host_is_superhost is not null": "host_is_superhost IS NOT NULL",
                         "host_is_superhost empty string no space": "host_is_superhost != ''",
                         "host_is_superhost empty string space": "host_is_superhost != ' '" 
                         })
@dlt.expect_or_fail("cancellation_policy_not_null", "cancellation_policy IS NOT NULL")
@dlt.expect_or_fail("cancellation_policy_empty_str", "cancellation_policy != ' ' ")
@dlt.expect_or_fail("neighbourhood_cleansed_not_null", "neighbourhood_cleansed IS NOT NULL")
@dlt.expect_or_fail("neighbourhood_cleansed_empty_str", "neighbourhood_cleansed != ' '")
@dltwithdebug(globals())
def data_validation_gold_layer() -> pyspark.sql.dataframe.DataFrame:

    """This function reads the raw data from source. This version reads from internal database and returns a bronze dataframe.

    :return: pyspark.sql.dataframe.DataFrame

    """
    # The version of data to be runed is controlled via the Configuration in pipeline settings of the DLT
    # We have the possibility run the test data or the skewed test data
    # The run_data_version is not possible in the test, here will we use the mock dataset random_df
    # run_data_version = spark.conf.get("run_data_version)

    gold_df = spark.table("dlt_integration_test.gold_dlt_table")

    return gold_df
    
# See the output
showoutput(data_validation_gold_layer)
