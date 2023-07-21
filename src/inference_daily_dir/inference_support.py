import mlflow
import evidently
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset
import delta.tables as DT
import pyspark.sql.types as T
import pyspark.sql.functions as F
import pyspark
from datetime import date

from src.common_dir.common_functions import Common
from src.attributes_dir import attributes as A

class InferenceSupportClass:
    """
    This class contains supports functions used when for doing inference. 
    """

    def merge_into_fn(self, temp_df) -> None:

        """
        Applies merge into for new data. Merge into updates the row if the join condition already exists and inserts a new row if it does not.

        :param temp_df: The spark dataframe to be checked. 
        :type temp_df: pyspark.sql.dataframe.DataFrame
        """

        #Create SparkSession, needed when using repos. 
        spark = Common.create_spark_session()

        # First check if table exists, else create
        if spark.catalog.tableExists(A.TableNames.unseen_data_passed_to_model) is not True:
            temp_df.write.format("delta").mode("overwrite").saveAsTable(A.TableNames.unseen_data_passed_to_model)

        # If table exists, then merge
        if spark.catalog.tableExists(A.TableNames.unseen_data_passed_to_model) :

            tbl=DT.DeltaTable.forName(spark,tableOrViewName=A.TableNames.unseen_data_passed_to_model)
            
            # Very unlikely for two rows to have the same longitude and latitude
            join_cond = "original.longitude = updates.longitude and original.latitude = updates.latitude"
            col_dct={}

            for col in temp_df.columns:
                col_dct[f"{col}"]=f"updates.{col}"

            tbl.alias("original").merge(temp_df.alias("updates"),join_cond)\
            .whenMatchedUpdate(set=col_dct)\
            .whenNotMatchedInsert(values=col_dct)\
            .execute()

    def check_if_unseeen_data_is_passed_to_model_fn(self, daily_df, daily_pred_df) -> None:
        """
        This function checks if unseen data is passed to the model. The model pipeline ignores rows which it has not been trained on. Therefore may the prediciton not be of the same length as the incoming data.
        If subtracted_df is empty it means that no unseen data was passed to the model. 

        :param daily_df: Incoming data which is of gold standard. 
        :type daily_df: pyspark.sql.dataframe.DataFrame
        :param daily_pred_df: The predicted data.  
        :type daily_pred_df: pyspark.sql.dataframe.DataFrame
        """

        original_columns = daily_df.columns
        filtered_daily_pred_df = daily_pred_df.select(original_columns)

        subtracted_df = daily_df.subtract(filtered_daily_pred_df)
        
        if subtracted_df.isEmpty() is not True:
            self.merge_into_fn(temp_df=subtracted_df)

        else:
            print("No unseen data passed to the pipeline for inference")


    def data_drift_fn(self, daily_df, reference_data_data_drift_df) -> None:

        """
        Measures the data drift of the incoming data.

        :param daily_df: Incoming data which is of gold standard. 
        :type daily_df: pyspark.sql.dataframe.DataFrame
        :param reference_data_data_drift_df: The reference data which the model have been trained on.  
        :type reference_data_data_drift_df: pyspark.sql.dataframe.DataFrame
        """

        data_drift_report = Report(metrics=[DataDriftPreset()])
        data_drift_report.run(current_data=daily_df.toPandas(), reference_data=reference_data_data_drift_df.drop("price").toPandas(), column_mapping=None)

        # Saveing to dbfs and read back. Otherwise it did not work to display in Databricks
        data_drift_report.save_html(f"/dbfs/FileStore/data_drift_report/{date.today()}.html")

        # Save report to table
        self._data_drift_to_tbl_fn(data_drift_report)

    def _data_drift_to_tbl_fn(self, data_drift_report) -> None:

        """
        A support function to data_drift_fn, which saves the data drift report to a table. 

        :param data_drift_report: The Evidently data drift report, exluding target variable  
        :type data_drift_report: evidently.report.report.Report
        """

        #Create SparkSession, needed when using repos. 
        spark = Common.create_spark_session()

        data_drift_report_dct = data_drift_report.as_dict()
        
        # Save boolean flag as table to create an alerter in SQL dashboard
        data_drift_df = spark.createDataFrame(
            [
                (f"{date.today()}", data_drift_report_dct["metrics"][0]["result"]["dataset_drift"]),  # Add your data here
            ],
            T.StructType(  # Define the whole schema within a StructType
                [
                    T.StructField("date", T.StringType(), True),
                    T.StructField("drift_detected", T.BooleanType(), True),
                ]
            ),
        )

        # Saving df as a table so it can be read in SQL Dasboard and call an alert if drift is detected
        data_drift_df.write.format("delta").mode("overwrite").saveAsTable(A.TableNames.data_drift_df)

    def model_drift_fn(self, daily_pred_df, reference_data_data_drift_df) -> None:

        """
        Measures the model drift i.e. if the predicted data is not accurate anymore. 

        :param daily_pred_df: The daily predictions. 
        :type daily_pred_df: pyspark.sql.dataframe.DataFrame
        :param reference_data_data_drift_df: The reference data which the model have been trained on.  
        :type reference_data_data_drift_df: pyspark.sql.dataframe.DataFrame
        """

        model_drift_report = Report(metrics=[DataDriftPreset()])
        model_drift_report.run(current_data=daily_pred_df.select("price").toPandas(), reference_data=reference_data_data_drift_df.select("price").toPandas())

        model_drift_report.save_html(f"/dbfs/FileStore/model_drift_report/{date.today()}.html")

        # Save report to table
        self._model_drift_to_tbl_fn(model_drift_report)

    def _model_drift_to_tbl_fn(self, model_drift_report) -> None:

        """
        A support function to model_drift_fn, which saves the model drift report to a table. 

        :param model_drift_report: The Evidently data drift report, only target variable  
        :type model_drift_report: evidently.report.report.Report
        """

        #Create SparkSession, needed when using repos. 
        spark = Common.create_spark_session()
        
        # Save boolean flag as table to create an alerter in SQL dashboard
        model_drift_report_dct = model_drift_report.as_dict()

        model_drift_df = spark.createDataFrame(
            [
                (f"{date.today()}", model_drift_report_dct["metrics"][0]["result"]["dataset_drift"]), 
            ],
            T.StructType(  # Define the whole schema within a StructType
                [
                    T.StructField("date", T.StringType(), True),
                    T.StructField("model_drift_detected", T.BooleanType(), True),
                ]
            ),
        )

        model_drift_df.write.format("delta").mode("overwrite").saveAsTable(A.TableNames.model_drift_df)

    def cast_data_fn(self, daily_df) -> pyspark.sql.dataframe.DataFrame:
        """
        This function will act as a safeguard and cast the data into certain datatypes if they are not already of the specific data types that the model was trained on. This could for an example happen when we read CSV files from the Azure Data Lake Storage.

        :param daily_df: Incoming data which is of gold standard and should be converted. 
        :type daily_df: pyspark.sql.dataframe.DataFrame

        :returns: A transformed dataframe with correct data types that corresponds to the training data 
        :rtype: pyspark.sql.dataframe.DataFrame
        """

        daily_df_converted = daily_df.select(
            F.col("host_is_superhost"),
            F.col("cancellation_policy"),
            F.col("instant_bookable"),
            F.col("host_total_listings_count").cast("double"),
            F.col("neighbourhood_cleansed"),
            F.col("latitude").cast("double"),
            F.col("longitude").cast("double"),
            F.col("property_type"),
            F.col("room_type"),
            F.col("accommodates").cast("double"),
            F.col("bathrooms").cast("double"),
            F.col("bedrooms").cast("double"),
            F.col("beds").cast("double"),
            F.col("bed_type"),
            F.col("minimum_nights").cast("double"),
            F.col("number_of_reviews").cast("double"),
            F.col("review_scores_rating").cast("double"),
            F.col("review_scores_accuracy").cast("double"),
            F.col("review_scores_cleanliness").cast("double"),
            F.col("review_scores_checkin").cast("double"),
            F.col("review_scores_communication").cast("double"),
            F.col("review_scores_location").cast("double"),
            F.col("review_scores_value").cast("double"),
            F.col("aggregated_review_scores").cast("string"),
            F.col("price").cast("double")
        )

        return daily_df_converted
    
    def subtract_review_score(self, daily_df) -> pyspark.sql.dataframe.DataFrame:
        """
        This function subtract the review_scores_rating with the aggregated score.  

        :param daily_df: Incoming data which is of gold standard. 
        :type daily_df: pyspark.sql.dataframe.DataFrame

        :returns: A transformed dataframe with correct data types that corresponds to the training data 
        :rtype: pyspark.sql.dataframe.DataFrame
        """

        daily_df_converted = daily_df.withColumn("aggregated_review_scores", F.col("aggregated_review_scores") - F.col("review_scores_rating"))

        return daily_df_converted