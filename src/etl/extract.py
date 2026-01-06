"""
ETL Module — Extract
Reads raw data from CSV/JSON sources and returns a PySpark DataFrame.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
import logging

logger = logging.getLogger(__name__)


def create_spark_session(app_name: str = "ETL_Pipeline") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def extract_csv(spark: SparkSession, path: str, schema: StructType = None) -> DataFrame:
    """Load a CSV file into a DataFrame, with optional schema enforcement."""
    logger.info(f"Extracting CSV from: {path}")
    reader = spark.read.option("header", "true").option("inferSchema", str(schema is None).lower())
    if schema:
        reader = reader.schema(schema)
    df = reader.csv(path)
    logger.info(f"Extracted {df.count()} rows from {path}")
    return df


def extract_json(spark: SparkSession, path: str, schema: StructType = None) -> DataFrame:
    """Load a JSON file into a DataFrame."""
    logger.info(f"Extracting JSON from: {path}")
    reader = spark.read
    if schema:
        reader = reader.schema(schema)
    df = reader.json(path)
    logger.info(f"Extracted {df.count()} rows from {path}")
    return df


def extract_parquet(spark: SparkSession, path: str) -> DataFrame:
    """Load a Parquet file into a DataFrame."""
    logger.info(f"Extracting Parquet from: {path}")
    df = spark.read.parquet(path)
    logger.info(f"Extracted {df.count()} rows from {path}")
    return df
