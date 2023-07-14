#import sys
#import os

# This row allows importing modules from folders
#sys.path.append(os.path.abspath('/Workspace/Repos/andreas.forsberg@capgemini.com/mvp_ml_delivery'))

import pyspark
from pyspark.sql import SparkSession

import pyspark.sql.types as T
import pyspark.sql as S
import pyspark.sql.functions as F

from src.medallion_dir import imedallion as IM
from src.attributes_dir import attributes as A
from src.common_dir import common_functions as C
# from src.medallion_dir import support_functions as SF
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

        :param frac: The fraction the data should be sampled into. Use 1.0 for full dataset. 
        :type frac: float

        :param s: The seed parameter of the sample function. 
        :type s: int

        :return: A spark dataframe with bronze data
        :rtype: S.dataframe.DataFrame"""

        #Create SparkSession, needed when using repos. 
        spark = C.Common.create_spark_session()
        
        bronze_df = spark.table(tbl_nm)
        bronze_df = bronze_df.sample(fraction=fraction, seed=seed)

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

        return silver_df

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

        # Change order of columns and put target last
        cols = gold_df.columns
        cols.remove(A.AttributesTarget.price.name)

        ordered_gold_df = gold_df.select([*cols, A.AttributesTarget.price.name])

        return ordered_gold_df