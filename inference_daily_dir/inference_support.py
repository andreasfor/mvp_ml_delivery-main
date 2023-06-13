import mlflow
import evidently
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset
import delta.tables as DT
import pyspark.sql.types as T

from datetime import date

from attributes_dir import attributes as A
from common_dir import common as C

class InferenceSupportClass:

    def merge_into_fn(self,temp_df) -> None:

        #Create SparkSession, needed when using repos. 
        spark = C.Common.create_spark_session()

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

    def check_if_unseeen_data_is_passed_to_model_fn(self,daily_df, daily_pred_df):

        original_columns = daily_df.columns
        filtered_daily_pred_df = daily_pred_df.select(original_columns)

        subtracted_df = daily_df.subtract(filtered_daily_pred_df)
        
        if subtracted_df.isEmpty() is not True:
            self.merge_into_fn(temp_df=subtracted_df)

        else:
            print("No unseen data passed to the pipeline for inference")


    def data_drift_fn(self, daily_df, reference_data_data_drift_df) -> None:

        data_drift_report = Report(metrics=[DataDriftPreset()])
        data_drift_report.run(current_data=daily_df.toPandas(), reference_data=reference_data_data_drift_df.drop("price").toPandas(), column_mapping=None)

        # Saveing to dbfs and read back. Otherwise it did not work to display in Databricks
        data_drift_report.save_html(f"/dbfs/FileStore/data_drift_report/{date.today()}.html")

        # Save report to table
        self._data_drift_to_tbl_fn(data_drift_report)

    def _data_drift_to_tbl_fn(self, data_drift_report) -> None:

        #Create SparkSession, needed when using repos. 
        spark = C.Common.create_spark_session()

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

        model_drift_report = Report(metrics=[DataDriftPreset()])
        model_drift_report.run(current_data=daily_pred_df.select("price").toPandas(), reference_data=reference_data_data_drift_df.select("price").toPandas())

        model_drift_report.save_html(f"/dbfs/FileStore/model_drift_report/{date.today()}.html")

        # Save report to table
        self._model_drift_to_tbl_fn(model_drift_report)

    def _model_drift_to_tbl_fn(self, model_drift_report) -> None:

        #Create SparkSession, needed when using repos. 
        spark = C.Common.create_spark_session()
        
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