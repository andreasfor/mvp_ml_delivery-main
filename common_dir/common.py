import pyspark.sql as S

class Common:

    @staticmethod
    def create_spark_session():
        spark = (S.SparkSession
                 .builder
                 .appName("FetchingSpark")
                 .getOrCreate())
        return spark