import json

def mount_to_adls_fn() -> None:

    """
    Note that I am reading from a txt file that holds my secrets. This file is not pushed to GitHub since I have added it to gitignore. Please note that this is not the best way of storing secrets but is a work around for now since I do not have admin rights to create an Azure Key Vault. 
    """
    
    # Reading the data from the file
    with open('/Workspace/Repos/andreas.forsberg@capgemini.com/mvp_ml_delivery-main/authorization_adls.txt') as f:
        data = f.read()
        
    # Reconstructing the data as a dictionary
    authorization_dct = json.loads(data)

    # Code from ChatGPT
    storage_account_name = authorization_dct["storage_account_name"]
    storage_account_key = authorization_dct["key"] 
    container_name = "airbnb"
    mount_point = "/mnt/azure_data_lake/airbnb"

    # Unmount the Blob storage if it's already mounted
    # Comment out if it is the first time mounting
    dbutils.fs.unmount(mount_point)

    # Mount the Blob storage
    dbutils.fs.mount(
        source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
        mount_point=mount_point,
        extra_configs={
            f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_key
        }
    )

    spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")


def _get_dbutils(spark):
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
        except ImportError:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]
        return dbutils

from src.common_dir import common_functions as C
spark = C.Common.create_spark_session()

dbutils = _get_dbutils(spark)


# mount_to_adls_fn()