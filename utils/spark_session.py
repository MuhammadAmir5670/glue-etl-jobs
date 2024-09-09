from pyspark.sql import SparkSession


def create_spark_context():
    spark = SparkSession.builder.appName(__name__).getOrCreate()

    return spark
