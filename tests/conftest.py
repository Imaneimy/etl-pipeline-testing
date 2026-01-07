import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="function")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("ETL_Tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
