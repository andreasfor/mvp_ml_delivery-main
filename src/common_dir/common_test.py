import pyspark.sql as S

def test_import_basics() -> None:
    """
    This test will just import the common module and check if it exists.
    """

    try:
        import importlib.util
        imp = importlib.util.find_spec("common_dir.common", package="Common")
        found = imp is not None
        assert found
    except:
        assert False

def test_call_spark_session() -> None:
    
    """
    This test will call the spak session and verify it is of the corret type.
    """

    from common_dir import common

    spark = common.Common.create_spark_session()

    assert isinstance(spark, S.session.SparkSession)
    