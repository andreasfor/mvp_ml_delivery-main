# Databricks notebook source
from runtime.nutterfixture import NutterFixture, tag

class MyTestFixture(NutterFixture):

    def assertion_import_basics(self) -> None:
        """
        This test will just import the ml modules and check if they exists.
        """
        try:
            import importlib.util
            imp_m = importlib.util.find_spec("src.medallion_dir.medallion", package="Medallion")
            found_m = imp_m is not None
            assert found_m
        except:
            assert False

        try:
            import importlib.util
            imp_mf = importlib.util.find_spec("src.medallion_dir.medallion_factory", package="MedallionFactory")
            found_mf = imp_mf is not None
            assert found_mf
        except:
            assert False

        try:
            import importlib.util
            imp_im = importlib.util.find_spec("src.medallion_dir.imedallion", package="IMedallion")
            found_im = imp_im is not None
            assert found_im
        except:
            assert False
    
    def assertion_bronze_from_RAW_INTERNAL_DATABASE_to_gold_medallion_component(self) -> None:
        """This test will use the Medallion component and read the raw data and convert it to gold. These tests are very similar to the DLT integration tests based on mock data.
           The data that is read is mock data"""
        
        try:
            import json

            import pyspark.sql as S
            import pyspark.sql.types as T
            import pyspark.sql.functions as F

            from src.attributes_dir import attributes as A
            import src.medallion_dir.medallion_factory as MF
            import src.medallion_dir.imedallion as IM
            from src.medallion_dir.support_functions import create_mock_dataset

            medallion = MF.MedallionFactory.create_or_get(
                version = MF.MedallionFactory.Version.V1,
                call = IM.IMedallion.Call.RAW_INTERNAL_DATABASE)
            
            table_name_of_mock_dataset_str = create_mock_dataset()

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


