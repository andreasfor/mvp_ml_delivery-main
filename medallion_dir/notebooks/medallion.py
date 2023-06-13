# Databricks notebook source
# MAGIC %run ../attributes

# COMMAND ----------

# MAGIC %run ./imedallion

# COMMAND ----------

import pyspark.sql.types as T
import pyspark.sql as S

# COMMAND ----------

class Medallion(IMedallion):

    """This class transforms the raw data into curated data according to the medallion structure"""

    def __init__(self):
        pass
    
    @staticmethod
    @udf(returnType=T.StringType())
    def _aggregate_reviews(review_scores_accuracy, review_scores_cleanliness, review_scores_checkin, review_scores_communication, review_scores_location, review_scores_value):

        aggregated_value = review_scores_accuracy + review_scores_cleanliness + review_scores_checkin + review_scores_communication + review_scores_location + review_scores_value

        return aggregated_value

    
    def imedallion_raw_to_bronze_transformation(self, tbl_nm, fraction, seed) -> S.dataframe.DataFrame:

        """This function reads the raw data from source. This version reads from internal database and returns a bronze dataframe.
        
        :param tbl_nm: The table name of the raw data 
        :type tbl_nm: string

        :param fraction: The fraction the data should be sampled into. Use 1.0 for full dataset. 
        :type fraction: float

        :param seed: The seed parameter of the sample function. 
        :type seed: int

        :return: S.dataframe.DataFrame

        :raise: ?: ?
        
        """
        bronze_df = spark.table(tbl_nm)
        bronze_df = bronze_df.sample(fraction=fraction, seed=seed)

        return bronze_df


    def imedallion_bronze_to_silver_transformation(self, bronze_df) -> S.dataframe.DataFrame:
        
        """This function reads cleans the bronze dataframe by removing duplicates and removing nan and returns a silver dataframe.
        
        :param bronze_df: The bronze dataframe 
        :type bronze_df: S.dataframe.DataFram

        :return: S.dataframe.DataFrame

        :raise: ?: ?"""

        silver_df = bronze_df.dropDuplicates().dropna()

        return silver_df

    def imedallion_silver_to_gold_transformation(self, silver_df) -> S.dataframe.DataFrame:

        """This function aggregates review scores of the silver dataframe and returns a gold dataframe.
        
        :param silver_df: The silver dataframe 
        :type silver_df: S.dataframe.DataFram

        :return: S.dataframe.DataFrame

        :raise: ?: ?"""
        
        gold_df = silver_df.withColumn(AttributesAdded.aggregated_review_scores.name, self._aggregate_reviews(AttributesOriginal.review_scores_accuracy.name, AttributesOriginal.review_scores_cleanliness.name, AttributesOriginal.review_scores_checkin.name, AttributesOriginal.review_scores_communication.name,AttributesOriginal.review_scores_location.name, AttributesOriginal.review_scores_value.name))

        # Change order of columns and put target last
        cols = gold_df.columns
        cols.remove(AttributesTarget.price.name)

        ordered_gold_df = gold_df.select([*cols, AttributesTarget.price.name])

        return ordered_gold_df

# COMMAND ----------


