import pyspark.sql as S

class Common:
    """
    The purpose of the Common class is to facilitate hygenic functions that is generall for the project to work. 
    """

    @staticmethod
    def create_spark_session() -> S.SparkSession:
        """
        Creates a spark session needed when using repos and not workspace. 
        
        :returns: Spark session
        :rtype: pyspark.sql.SparkSession
        """
        spark = (S.SparkSession
                 .builder
                 .appName("FetchingSpark")
                 .getOrCreate())
        return spark