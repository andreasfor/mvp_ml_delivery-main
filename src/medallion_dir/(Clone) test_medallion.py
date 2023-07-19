# Databricks notebook source
from runtime.nutterfixture import NutterFixture, tag

class MyTestFixture(NutterFixture):


    def assertion_bronze_from_RAW_EXTERNAL_DATABASE_ADLS_to_gold_medallion_component(self) -> None:
        """This test will use the Medallion component and read the raw data from the external database and convert it to gold.
        The data that is read is mock data"""
        
        try:
            import pyspark.sql as S
            import pyspark.sql.types as T
            import pyspark.sql.functions as F

            from src.attributes_dir import attributes as A
            import src.medallion_dir.medallion_factory as MF
            import src.medallion_dir.imedallion as IM
            import src.medallion_dir.support_functions as SF
            from src.common_dir import common_functions as C

            medallion = MF.MedallionFactory.create_or_get(
                version = MF.MedallionFactory.Version.V1,
                call = IM.IMedallion.Call.RAW_INTERNAL_DATABASE)

            bronze_df = medallion.imedallion_read_adls_merge_raw_into_bronze_transformation(mnt_path="dbfs:/mnt/azure_data_lake/airbnb/test_airbnb_1.csv", test_mode=True)

            # Verify that bronze df contains 22 rows
            try:
                assert bronze_df.count() == 22
            except:
                raise Exception("Test fail due to length of bronze dataframe is not as exptected")
            
            silver_df = medallion.imedallion_bronze_to_silver_transformation(bronze_df=bronze_df)

            # Verify that silver df contain 2 row after removing duplicates and nan
            try:
                assert silver_df.count() == 2
            except:
                raise Exception("Test fail due to length of silver dataframe is not as expected after dropping rows with duplicates and nan values")

            gold_df = medallion.imedallion_silver_to_gold_transformation(silver_df=silver_df)

            # Verify that the aggreagted value from the UDF is 7 for 2 rows
            try:
                assert gold_df.filter(F.col(A.AttributesAdded.aggregated_review_scores.name) == 7).count() == 2
            except:
                raise Exception("Test fail due to length of gold dataframe is not as expected after aggregating review scores")

        except:
            assert False
    

result = MyTestFixture().execute_tests()
print(result.to_string())
# Comment out the next line (result.exit(dbutils)) to see the test result report from within the notebook
# result.exit(dbutils)

# COMMAND ----------

bronze_df = spark.table("default.test_adls_bronze_layer")
bronze_df.display()

# COMMAND ----------

bronze_df.printSchema()

# COMMAND ----------

import pyspark.sql.functions as F
from functools import reduce

# COMMAND ----------

silver_df = bronze_df.dropDuplicates().dropna()

categorical_cols = [field for (field, dataType) in silver_df.dtypes if dataType == "string"]

# filter out rows with blank strings in all the columns
no_space_silver_df = silver_df.filter(reduce(lambda x, y: x & y, [(F.col(c) != ' ') & (F.col(c) != '') for c in categorical_cols]))

# COMMAND ----------

no_space_silver_df.display()

# COMMAND ----------

import pyspark.sql as S
import pyspark.sql.types as T
import pyspark.sql.functions as F

from src.attributes_dir import attributes as A
import src.medallion_dir.medallion_factory as MF
import src.medallion_dir.imedallion as IM
import src.medallion_dir.support_functions as SF
from src.common_dir import common_functions as C

medallion = MF.MedallionFactory.create_or_get(
    version = MF.MedallionFactory.Version.V1,
    call = IM.IMedallion.Call.RAW_INTERNAL_DATABASE)

# COMMAND ----------

gold_df = medallion.imedallion_silver_to_gold_transformation(silver_df=no_space_silver_df)

# COMMAND ----------

gold_df.display()

# COMMAND ----------

gold_df.filter(F.col(A.AttributesAdded.aggregated_review_scores.name) == 7).count()

# COMMAND ----------

mnt_path = "dbfs:/mnt/azure_data_lake/airbnb/test_airbnb_1.csv"

# COMMAND ----------

import src.medallion_dir.support_functions as SF

# COMMAND ----------

SF.mount_path_exists_fn(mnt_path=mnt_path)

# COMMAND ----------

SF.mount_to_adls_fn(mnt_path)

# COMMAND ----------

from src.common_dir import common_functions as C

#Create SparkSession, needed when using repos. 
spark = C.Common.create_spark_session()

test_mode = True

test = ""

if test_mode:
    test = "test_"

    # Drop the table "default.airbnb"
    spark.sql(f"DROP TABLE default.{test}adls_bronze_layer")

# COMMAND ----------

spark.table("default.test_adls_bronze_layer")

# COMMAND ----------

spark.catalog.tableExists("default.test_adls_bronze_layer")

# COMMAND ----------


