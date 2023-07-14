# Databricks notebook source
import pyspark.sql as S
from runtime.nutterfixture import NutterFixture, tag

class MyTestFixture(NutterFixture):

    def assertion_import_basics(self) -> None:
        """
        This test will just import the common module and check if it exists.
        """

        try:
            import importlib.util
            imp = importlib.util.find_spec("src.common_dir.common_functions", package="Common")
            found = imp is not None
            assert found
        except:
            assert False

    def assertion_call_spark_session(self) -> None:
        """
        This test will call the spak session and verify it is of the corret type.
        """

        import src.common_dir.common_functions as C

        spark = C.Common.create_spark_session()

        assert isinstance(spark, S.session.SparkSession)

result = MyTestFixture().execute_tests()
print(result.to_string())
# Comment out the next line (result.exit(dbutils)) to see the test result report from within the notebook
# result.exit(dbutils)

# COMMAND ----------


