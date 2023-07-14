# Databricks notebook source
import os
import sys
sys.path.append(os.path.abspath('/../../../mvp_ml_delivery'))

import pyspark.sql as S
from runtime.nutterfixture import NutterFixture, tag

class MyTestFixture(NutterFixture):

    def assertion_run_integration_test_of_DLT_pipeline(self) -> None:
        """
        This test will call the job via Job API, from workflows, which will run the integraton tests for the DLT pipeline.  
        """

        try:
            import requests
            import json
            import time

            #This will trigger the job programmatically and then will we extract the run_id

            my_json = {"job_id": "41409489135749"}    
            auth = {"Authorization": "Bearer dapi9756c2d167c4ff39ccf50203c0f56fa4-2"}
            response = requests.post('https://adb-6677420654375794.14.azuredatabricks.net/api/2.0/jobs/run-now', json = my_json, headers=auth).json()

            # The run id should be placed here and to retreive the status of the job.
            # We are using a timer which will re-start untill we get a key called 'result_state'

            api_url = "https://adb-6677420654375794.14.azuredatabricks.net/api/2.0/jobs/runs/get"

            headers = {
                "Authorization": "Bearer dapi9756c2d167c4ff39ccf50203c0f56fa4-2",
                "Content-Type": "application/json"}

            run_id=str(response["run_id"])
            #run_id = "104024"
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

response.json()["state"].keys():

# COMMAND ----------

#This will trigger the job programmatically and then will we extract the run_id

import requests
import json

my_json = {"job_id": "41409489135749"}    

auth = {"Authorization": "Bearer dapi9756c2d167c4ff39ccf50203c0f56fa4-2"}

response = requests.post('https://adb-6677420654375794.14.azuredatabricks.net/api/2.0/jobs/run-now', json = my_json, headers=auth).json()
print(response)

# COMMAND ----------

str(response["run_id"])

# COMMAND ----------

# The run id should be placed here and to retreive the status of the job.
# We are using a timer which will re-start untill we get a key called 'result_state'

import requests
import json
import time

api_url = "https://adb-6677420654375794.14.azuredatabricks.net/api/2.0/jobs/runs/get"

headers = {
    "Authorization": "Bearer dapi9756c2d167c4ff39ccf50203c0f56fa4-2",
    "Content-Type": "application/json"}

# run_id=str(response["run_id"])
run_id = "104024"
params = {"run_id": run_id}

response = requests.get(api_url, headers=headers, params=params)

timeout = 60*30   # 30 minutes
timeout_start = time.time()

while (time.time() < timeout_start + timeout):
    
    if "result_state" in response.json()["state"].keys():
        assert response.json()["state"]["result_state"] == "SUCCESS"
        print("WOHO WE DID IT")
        break

    print(time.time())

    time.sleep(1)

# COMMAND ----------

import time

timeout = 60*30   # 30 minutes

timeout_start = time.time()

while (time.time() < timeout_start + timeout):
    
    if "result_state" in response.json()["state"].keys():
        assert response.json()["state"]["result_state"] == "SUCCESS"
        break

    print(time.time())

    time.sleep(1)

# COMMAND ----------

response.json()["state"]["result_state"]

# COMMAND ----------

# Keep running with e.g. sleep unless the result_state is "result_state': 'FAILED'" or Succeeded

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.types import IntegerType
from pyspark.sql.types import *
from pyspark.sql import Row
import base64
import requests
import json

# COMMAND ----------

import requests
import json

my_json = {"job_id": "41409489135749"}    

auth = {"Authorization": "Bearer dapi9756c2d167c4ff39ccf50203c0f56fa4-2"}

response = requests.post('https://adb-6677420654375794.14.azuredatabricks.net/api/2.0/jobs/run-now', json = my_json, headers=auth).json()
print(response)

# COMMAND ----------

from pyspark.sql.types import IntegerType
from pyspark.sql.types import *
from pyspark.sql import Row
import base64
import requests
import json

databricks_instance ="https://adb-6677420654375794.14.azuredatabricks.net"

job_id = "41409489135749"

url_list = f"{databricks_instance}/api/2.1/jobs/get?job_id={job_id}"

headers = {
  'Authorization': 'Bearer dapi9756c2d167c4ff39ccf50203c0f56fa4-2',
  'Content-Type': 'application/json'
}

response = requests.request("GET", url_list, headers=headers).json()
print(response)

# COMMAND ----------

from pyspark.sql.types import IntegerType
from pyspark.sql.types import *
from pyspark.sql import Row
import base64
import requests
import json

databricks_instance ="https://adb-6677420654375794.14.azuredatabricks.net"

job_id = "41409489135749"

url_list = f"{databricks_instance}/api/2.1/jobs/get?job_id={job_id}"

headers = {
  'Authorization': 'Bearer dapi9756c2d167c4ff39ccf50203c0f56fa4-2',
  'Content-Type': 'application/json'
}

response = requests.get(url_list, headers=headers).json()
print(response)

# COMMAND ----------


api_url = "https://adb-6677420654375794.14.azuredatabricks.net/api/2.0/jobs/runs/get"

headers = {
    "Authorization": "Bearer dapi9756c2d167c4ff39ccf50203c0f56fa4-2",
    "Content-Type": "application/json"
}

params = {
    "run_id": "103037"
}

response = requests.get(api_url, headers=headers, params=params)
response.json()

# COMMAND ----------

response.json()

# COMMAND ----------

response["job_id"]

# COMMAND ----------

import requests

job_run_id = "41409489135749"
api_url = "https://adb-6677420654375794.14.azuredatabricks.net/api/2.0/jobs/runs/get"

headers = {
    "Authorization": "Bearer <YOUR_ACCESS_TOKEN>",
    "Content-Type": "application/json"
}

params = {
    "run_id": job_run_id
}

response = requests.get(api_url, headers=headers, params=params)
data = response.json()

# Extract the run status from the response
run_status = data["state"]["life_cycle_state"]
print("Run Status:", run_status)


# COMMAND ----------

res = get_run_status(run_id = 41409489135749,
                      workspace = "https://adb-6677420654375794.14.azuredatabricks.net",
                      token = "dapi9756c2d167c4ff39ccf50203c0f56fa4-2")

# COMMAND ----------

# MAGIC %sh
# MAGIC curl --netrc --get \
# MAGIC https://adb-6677420654375794.14.azuredatabricks.net/api/2.0/jobs/runs/get-output \
# MAGIC --data run_id=1030688924957943 \
# MAGIC | jq .

# COMMAND ----------


