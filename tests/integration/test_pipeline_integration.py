import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import pytest
from pyspark.sql.types import *
from etl.transform import normalize_column_names, drop_duplicates, handle_nulls, aggregate_sales
from validators.schema_validator import validate_schema, validate_not_null, validate_row_count


RAW_SCHEMA = StructType([
    StructField("date", StringType()),
    StructField("product_id", StringType()),
    StructField("region", StringType()),
    StructField("amount", StringType()),  # string avant cast — bug ici, montant pas casté
])

SILVER_SCHEMA = StructType([
    StructField("date", StringType()),
    StructField("product_id", StringType()),
    StructField("region", StringType()),
    StructField("amount", DoubleType()),
])


@pytest.fixture
def raw_df(spark):
    data = [
        ("2024-01-01", "P001", "NORTH", "150.00"),
        ("2024-01-01", "P001", "NORTH", "150.00"),
        ("2024-01-02", "P002", "SOUTH", "75.50"),
        ("2024-01-02", None, "EAST", "100.00"),
    ]
    return spark.createDataFrame(data, RAW_SCHEMA)


def test_pipeline_removes_duplicates(spark, raw_df):
    result = drop_duplicates(raw_df, subset=["date", "product_id", "region"])
    assert result.count() < raw_df.count()


def test_pipeline_drops_null_product_id(spark, raw_df):
    result = handle_nulls(raw_df, {"product_id": "drop"})
    from pyspark.sql import functions as F
    assert result.filter(F.col("product_id").isNull()).count() == 0


def test_pipeline_validates_row_count(spark, raw_df):
    result = validate_row_count(raw_df, min_rows=1)
    assert result.passed


# TC-INT-003 — ce test échouait avant le fix du cast
def test_pipeline_end_to_end(spark, raw_df):
    from etl.transform import cast_types
    df = normalize_column_names(raw_df)
    # manque le cast ici — amount reste string, agrégation va échouer
    df = drop_duplicates(df, subset=["date", "product_id", "region"])
    df = handle_nulls(df, {"product_id": "drop"})
    result = validate_not_null(df, ["date", "region"])
    assert result.passed
