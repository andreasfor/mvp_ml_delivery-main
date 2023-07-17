# Databricks notebook source
import pyspark.sql as S
from runtime.nutterfixture import NutterFixture, tag

class MyTestFixture(NutterFixture):

    def assertion_run_integration_test_of_DLT_pipeline(self) -> None:
        """
        This test will call the job via Job API, from workflows, which will run the integraton tests for the DLT pipeline.  
        The job consists of three tasks, which are, the DLT pipeline, verification tests of the data with expectations (this is used for both testing and when checking daily ingest) and data validation test (asserts). 
        """

        try:
            import requests
            import json
            import time

            #This will trigger the job programmatically and then will we extract the run_id

            # Storing my key in a file and using gitignore to not psuh it to GitHub 
            with open("/Workspace/Repos/andreas.forsberg@capgemini.com/mvp_ml_delivery-main/authorization.txt") as my_file:
                for line in my_file:
                    authorization = line

            auth = {"Authorization": f"Bearer {authorization}"}

            my_json = {"job_id": "41409489135749"}
            response = requests.post('https://adb-6677420654375794.14.azuredatabricks.net/api/2.0/jobs/run-now', json = my_json, headers=auth).json()

            # The run id should be placed here and to retreive the status of the job.
            # We are using a timer which will re-start untill we get a key called 'result_state'

            api_url = "https://adb-6677420654375794.14.azuredatabricks.net/api/2.0/jobs/runs/get"

            headers = {
                "Authorization": f"Bearer {auth}",
                "Content-Type": "application/json"}

            run_id=str(response["run_id"])
            params = {"run_id": run_id}

            response = requests.get(api_url, headers=headers, params=params)

            timeout = 60*15  # 15 minutes
            timeout_start = time.time()

            while (time.time() < timeout_start + timeout) and ("result_state" not in response.json()["state"].keys()):
                
                # Needed inside of the loop in order to be updated
                response = requests.get(api_url, headers=headers, params=params)
                
                if "result_state" in response.json()["state"].keys():
                    assert response.json()["state"]["result_state"] == "SUCCESS"
                    break

                time.sleep(10)
            
        except:
            assert False

result = MyTestFixture().execute_tests()
print(result.to_string())
# Comment out the next line (result.exit(dbutils)) to see the test result report from within the notebook
# result.exit(dbutils)

# COMMAND ----------

with open("/Workspace/Repos/andreas.forsberg@capgemini.com/mvp_ml_delivery-main/authorization.txt") as my_file:
    for line in my_file:
        authorization = line
