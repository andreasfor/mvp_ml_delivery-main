import pyspark
from pyspark.sql import SparkSession

import pyspark.sql.types as T
import pyspark.sql as S
import pyspark.sql.functions as F
import delta.tables as DT
from functools import reduce
import json

from src.medallion_dir import imedallion as IM
from src.attributes_dir import attributes as A
from src.common_dir import common_functions as C
import src.medallion_dir.support_functions as SF


class Medallion(IM.IMedallion):
    """This class transforms the raw data into curated data according to the medallion structure"""

    def __init__(self):
        pass
    

    def imedallion_raw_to_bronze_transformation(self, tbl_nm, fraction, seed) -> S.dataframe.DataFrame:

        """
        This function reads the raw data from source. This version reads from internal database and returns a bronze dataframe.
        
        :param tbl_nm: The table name of the raw data 
        :type tbl_nm: string

        :param fraction: The fraction the data should be sampled into. Use 1.0 for full dataset. 
        :type fraction: float

        :param seed: The seed parameter of the sample function. 
        :type seed: int

        :return: A spark dataframe with bronze data
        :rtype: S.dataframe.DataFrame"""

        #Create SparkSession, needed when using repos. 
        spark = C.Common.create_spark_session()
        
        bronze_df = spark.table(tbl_nm)
        bronze_df = bronze_df.sample(fraction=fraction, seed=seed)

        return bronze_df
    
    
    def imedallion_read_adls_merge_raw_into_bronze_transformation(self, mnt_path: str, test_mode: bool) -> S.dataframe.DataFrame:

        """
        Applies merge into for data read from Azure Data Lake Storage. Merge into updates the row if the join condition already exists and inserts a new row if it does not.

        :param mnt_path: The path where data read from Azure Data Lake Storage can be accessed. Should refer to the first file.
        :type mnt_path: str

        :param test_mode: Indicated if test mode should be running or not
        :type test_mode: bool

        :return: A spark dataframe with bronze data
        :rtype: S.dataframe.DataFrame
        """

        #Create SparkSession, needed when using repos. 
        spark = C.Common.create_spark_session()

        # Establishes the connection between Databricks and ADLS
        SF.mount_to_adls_fn()

        test = ""

        if test_mode:
            test = "test_"

            try:
                spark.sql(f"DROP TABLE default.test_adls_bronze_layer")
            except:
                pass

        if not SF.mount_path_exists_fn(mnt_path, spark):
            raise FileNotFoundError(f"Please check that you provided a valid path to the first file (e.g. airbnb_1) in the mounted folder located in DBFS")
        
        # To break the loop if no matching file initially can not be found
        initial_trial_counter = 0

        while True:

            initial_trial_counter +=1
            if initial_trial_counter > 2:
                break

            # Extract file name of the last upsert
            last_read_input_file_str = SF.extract_file_nm_of_last_upsert_fn(test, spark)

            # If there is no input file or an already existing table, then create a table from the path from mounting folder
            if last_read_input_file_str is None or spark.catalog.tableExists(f"default.{test}adls_bronze_layer") is not True:
            
                df = (spark.read
                    .format("csv")
                    .option("sep", ",")
                    .option("header", True)
                    .load(mnt_path)
                    )
                
                # Add a column with the file name of the incoming file
                df_new = df.withColumn("input_file_name", F.input_file_name())
                
                df_new.write.format("delta").mode("overwrite").saveAsTable(f"default.{test}adls_bronze_layer")
            
            else:

                # Then check if there is a new file name to read from Azure Data Lake Storage and merge

                # Extract file name of the last upsert
                last_read_input_file_str = SF.extract_file_nm_of_last_upsert_fn(test, spark)

                new_data_df = SF.read_new_data_fn(last_read_input_file_str, test, spark)

                # To break the while loop
                if new_data_df.isEmpty():
                    break

                tbl=DT.DeltaTable.forName(spark, tableOrViewName=f"default.{test}adls_bronze_layer")
                
                # Very unlikely for two rows to have the same longitude and latitude
                join_cond = "original.longitude = updates.longitude and original.latitude = updates.latitude"
                col_dct={}

                for col in new_data_df.columns:
                    col_dct[f"{col}"]=f"updates.{col}"

                tbl.alias("original").merge(new_data_df.alias("updates"),join_cond)\
                .whenMatchedUpdate(set=col_dct)\
                .whenNotMatchedInsert(values=col_dct)\
                .execute() 

        bronze_df = spark.table(f"default.{test}adls_bronze_layer")

        return bronze_df


    def imedallion_bronze_to_silver_transformation(self, bronze_df) -> S.dataframe.DataFrame:
        
        """
        This function reads cleans the bronze dataframe by removing duplicates and removing nan and returns a silver dataframe.
        
        :param bronze_df: The bronze dataframe 
        :type bronze_df: S.dataframe.DataFram

        :return: A spark dataframe with silver data
        :rtype: S.dataframe.DataFrame
        """

        silver_df = bronze_df.dropDuplicates().dropna()

        categorical_cols = [field for (field, dataType) in silver_df.dtypes if dataType == "string"]

        # filter out rows with blank strings in all the columns
        no_space_silver_df = silver_df.filter(reduce(lambda x, y: x & y, [(F.col(c) != ' ') & (F.col(c) != '') for c in categorical_cols]))

        return no_space_silver_df

    def imedallion_silver_to_gold_transformation(self, silver_df) -> S.dataframe.DataFrame:

        """
        This function aggregates review scores of the silver dataframe and returns a gold dataframe.
        
        :param silver_df: The silver dataframe 
        :type silver_df: S.dataframe.DataFram

        :return: A spark dataframe with gold data
        :rtype: S.dataframe.DataFrame
        """

        # Register an UDF. Need to do this version of registration when using UDFs in pyspark DLT pipelines and in repos
        _aggregate_reviews_udf = F.udf(SF._aggregate_reviews, T.DoubleType()) 
        
        gold_df = silver_df.withColumn(A.AttributesAdded.aggregated_review_scores.name, _aggregate_reviews_udf(A.AttributesOriginal.review_scores_rating.name, A.AttributesOriginal.review_scores_accuracy.name, A.AttributesOriginal.review_scores_cleanliness.name, A.AttributesOriginal.review_scores_checkin.name, A.AttributesOriginal.review_scores_communication.name, A.AttributesOriginal.review_scores_location.name, A.AttributesOriginal.review_scores_value.name))

        '''# Change order of columns and put target last
        cols = gold_df.columns
        cols.remove(A.AttributesTarget.price.name)

        ordered_gold_df = gold_df.select([*cols, A.AttributesTarget.price.name])'''

        return gold_df