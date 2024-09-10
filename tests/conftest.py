import pytest

from pyspark.sql import SparkSession
from pyspark.sql import Row


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.config("spark.driver.host", "localhost").appName("pytest").getOrCreate()

    yield spark

    spark.stop()
