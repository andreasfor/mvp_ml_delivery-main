# Databricks notebook source
import os
import sys
sys.path.append(os.path.abspath('/Workspace/Repos/andreas.forsberg@capgemini.com/mvp_ml_delivery'))

from runtime.nutterfixture import NutterFixture, tag

class MyTestFixture(NutterFixture):

    def assertion_import_basics(self) -> None:
        """
        This test will just import the ml modules and check if they exists.
        """
        try:
            import importlib.util
            imp_m = importlib.util.find_spec("medallion", package="Medallion")
            found_m = imp_m is not None
            assert found_m
        except:
            assert False

        try:
            import importlib.util
            imp_mf = importlib.util.find_spec("medallion_factory", package="MedallionFactory")
            found_mf = imp_mf is not None
            assert found_mf
        except:
            assert False

        try:
            import importlib.util
            imp_im = importlib.util.find_spec("imedallion", package="IMedallion")
            found_im = imp_im is not None
            assert found_im
        except:
            assert False
    
    def assertion_bronze_to_gold_medallion_component(self) -> None:
        """This test will use the Medallion component and read the raw data and convert it to gold. These tests are very similar to the DLT integration tests based on mock data.
           The data that is read is mock data"""
        
        try:
            from attributes_dir import attributes as A
            import medallion_factory as MF
            import imedallion as IM
            from common_dir import common
            from support_functions import create_mock_dataset

            import pyspark.sql as S
            import pyspark.sql.types as T
            import pyspark.sql.functions as F

            medallion = MF.MedallionFactory.create_or_get(
                version = MF.MedallionFactory.Version.V1,
                call = IM.IMedallion.Call.RAW_INTERNAL_DATABASE)
            
            table_name_of_mock_dataset_str = create_mock_dataset()

            spark = common.Common.create_spark_session()

            bronze_df = medallion.imedallion_raw_to_bronze_transformation(tbl_nm=table_name_of_mock_dataset_str, fraction=1.0, seed=3)

            # Verify that bronze df contains 11 rows
            try:
                assert bronze_df.count() == 11
            except:
                raise Exception("Test fail due to length of bronze dataframe is not as exptected")
            
            silver_df = medallion.imedallion_bronze_to_silver_transformation(bronze_df=bronze_df)

            # Verify that silver df contain 1 row after removing duplicates and nan
            try:
                assert silver_df.count() == 1
            except:
                raise Exception("Test fail due to length of silver dataframe is not as expected after dropping rows with duplicates and nan values")

            gold_df = medallion.imedallion_silver_to_gold_transformation(silver_df=silver_df)

            # Verify that the aggreagted value from the UDF is 7
            try:
                assert gold_df.filter(F.col(A.AttributesAdded.aggregated_review_scores.name) == 7).count() == 1
            except:
                raise Exception("Test fail due to length of gold dataframe is not as expected after aggregating review scores")

        except:
            assert False
    

result = MyTestFixture().execute_tests()
print(result.to_string())
# Comment out the next line (result.exit(dbutils)) to see the test result report from within the notebook
# result.exit(dbutils)

# COMMAND ----------


