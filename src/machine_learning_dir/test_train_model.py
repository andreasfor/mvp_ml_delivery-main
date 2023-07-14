# Databricks notebook source
from runtime.nutterfixture import NutterFixture, tag

class MyTestFixture(NutterFixture):

    def assertion_train_load_and_call_ml_model_for_inference(self) -> None:
        """
        This test will train, and load and use the ml model based on a random dataset.
        Be aware that you need to use a cluster with runtime of 11.3. Otherwise you may get unexpected errors when loading the model.
        """

        try:
            import os
            import sys

            sys.path.append(os.path.abspath('/Workspace/Repos/andreas.forsberg@capgemini.com/mvp_ml_delivery'))

            import ml_support as ML
            from common_dir import common

            import pyspark.sql as S
            from random import randint, uniform
            import pyspark.sql.types as T
            import mlflow
            from mlflow.tracking import MlflowClient

            spark = common.Common.create_spark_session() # Needed for creating autodocumentation with Sphinx

            # Create random data
            random_data = []
            for _ in range(200):
                random_row = S.Row(
                    host_is_superhost=str(randint(0, 1)),
                    cancellation_policy=" ",
                    instant_bookable=str(randint(0, 1)),
                    host_total_listings_count=uniform(1, 10),
                    neighbourhood_cleansed=" ",
                    latitude=uniform(-90, 90),
                    longitude=uniform(-180, 180),
                    property_type="Random Property Type",
                    room_type="Random Room Type",
                    accommodates=uniform(1, 10),
                    bathrooms=uniform(1, 5),
                    bedrooms=uniform(1, 5),
                    beds=uniform(1, 5),
                    bed_type="Random Bed Type",
                    minimum_nights=randint(1, 10),
                    number_of_reviews=randint(0, 100),
                    review_scores_rating=uniform(0, 100),
                    review_scores_accuracy=uniform(0, 10),
                    review_scores_cleanliness=uniform(0, 10),
                    review_scores_checkin=uniform(0, 10),
                    review_scores_communication=uniform(0, 10),
                    review_scores_location=uniform(0, 10),
                    review_scores_value=uniform(0, 10),
                    aggregated_review_scores="Random Review Scores",
                    price=uniform(10, 100)
                )
                random_data.append(random_row)

            # Convert the random data to a DataFrame
            random_df = spark.createDataFrame(random_data)

            train_df, val_df, test_df = random_df.randomSplit([.6, .2, .2], seed=42)

            data = {"train_df": train_df, "val_df": val_df}
            run_name = "NUTTER_TEST_CASE_RANDOM_DATA"
            max_evals = 1

            search_space_dct = {}
            search_space_dct["numTrees"] = {"start": 2, "end": 10, "q": 1}
            search_space_dct["maxDepth"] = {"start": 10, "end": 30, "q": 1}

            ML.train_model(data_dct=data,run_name=run_name,num_evals=max_evals,search_space_dct=search_space_dct)

            # ------------------------------------
            # Load model
            # ------------------------------------

            client = MlflowClient()
            experiment_id="b1db19ebf831421db2e321cc4f298b71" # Refers to where the experiment is located i.e. test_train_model in Experiments tab

            experiments = client.search_runs(experiment_ids=experiment_id)

            runs = client.search_runs(experiment_id, order_by=["attributes.start_time desc"], max_results=1)
            runs[0].data.metrics

            run_id = runs[0].info.run_id

            model_path = f"runs:/{run_id}/trained_pipeline"
            loaded_model = mlflow.spark.load_model(model_path)

            predictions = loaded_model.transform(test_df) # This row generates a column called 'prediction'
            
            # The test should pass if the dataframe is not empty when selecting the newly generated column 'prediction'
            assert predictions.select("prediction").isEmpty() == False
        except:
            assert False


result = MyTestFixture().execute_tests()
print(result.to_string())
# Comment out the next line (result.exit(dbutils)) to see the test result report from within the notebook
# result.exit(dbutils)

# COMMAND ----------


