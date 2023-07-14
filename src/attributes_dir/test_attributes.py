# Databricks notebook source
from runtime.nutterfixture import NutterFixture, tag

class MyTestFixture(NutterFixture):

    def assertion_import_basics(self) -> None:
        """This test will just import the attributes module and check if it exists."""

        try:
            import importlib.util
            imp = importlib.util.find_spec("src.attributes_dir.attributes", package="AttributesOriginal")
            found = imp is not None
            assert found
        except:
            assert False

    def assertion_verify_each_original_attribute(self) -> None:
        """This test should verify all the different attributes. That they can be accessed and that they are strings.
        In this example we are only looking at the first attribute in AttributesOriginal.
        One should test all the attributes in each of the different subclasses of the attributes module.""" 

        import src.attributes_dir.attributes as A

        try:
            assert A.AttributesOriginal.accommodates.name == "accommodates"
        except:
            raise Exception("Could not import the original attribute accommodate from attributes")

        try:
            assert type(A.AttributesOriginal.accommodates.name) == str
        except:
            raise Exception(f"Accommodates from the attributes module is not of the type str, got {type(A.AttributesOriginal.accommodates.name)} instead")

    def assertion_negative_each_original_attribute(self) -> None:
        """
        This is a negative test of AttributesOriginal. Here we will verify that we get the expected failure if we inject errors.
        Note that we need to add assert False in the try. Since, if the test passes and NOT goes direclty to the except while we are injecting the test has failed. 
        """
        
        import src.attributes_dir.attributes as A
        
        if not type(A.AttributesOriginal.accommodates.name) == str:
            assert False

        if not A.AttributesOriginal.accommodates.name == "accommodates":
            assert False


result = MyTestFixture().execute_tests()
print(result.to_string())
# Comment out the next line (result.exit(dbutils)) to see the test result report from within the notebook
# result.exit(dbutils)

# COMMAND ----------

import src.attributes_dir.attributes as A
