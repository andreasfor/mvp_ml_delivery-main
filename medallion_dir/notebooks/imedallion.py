# Databricks notebook source
from enum import Enum, auto
import pyspark.sql as S

# COMMAND ----------

class IMedallion:
    """This class is the interface of the Medallion class. The main use for the interface is that we can hide functions from the end user.

    If a interface is published it should not be changed. Then we need to create a new one. In order to mitigate the risk of creating multiple interfaces, one can aim to create a more general interface. A more general interface has the downside of beeing harder to follow. One way of creating this interface in a more generic way is to only have one method that is called transform, which does all the transformations for us and then we can change things inside the transform function without affecting end user. """

    class Call(Enum):
        RAW_INTERNAL_DATABASE = auto()
    
    def imedallion_raw_to_bronze_transformation(self, tbl_nm, fraction, seed) -> S.dataframe.DataFrame:

        """This function reads the raw data from source. This version reads from internal database and returns a bronze datafra,me.
        
        :param tbl_nm: The table name of the raw data 
        :type tbl_nm: string

        :param frac: The fraction the data should be sampled into. Use 1.0 for full dataset. 
        :type frac: float

        :param s: The seed parameter of the sample function. 
        :type s: int

        :return: S.dataframe.DataFrame

        :raise: ?: ?
        
        """
        pass

    def imedallion_bronze_to_silver_transformation(self, bronze_df) -> S.dataframe.DataFrame:
        
        """This function reads cleans the bronze dataframe by removing duplicates and removing nan and returns a silver dataframe.
        
        :param bronze_df: The bronze dataframe 
        :type bronze_df: S.dataframe.DataFram

        :return: S.dataframe.DataFrame

        :raise: ?: ?"""

        pass

    def imedallion_silver_to_gold_transformation(self, silver_df) -> S.dataframe.DataFrame:

        """This function aggregates review scores of the silver dataframe and returns a gold dataframe.
        
        :param silver_df: The silver dataframe 
        :type silver_df: S.dataframe.DataFram

        :return: S.dataframe.DataFrame

        :raise: ?: ?"""
        
        pass

    def imedallion_raw_to_gold_dlt_transformation(self, tbl_nm, fraction, seed):
        pass

# COMMAND ----------


