"""
Unit Tests — ETL Transform module
XRAY IDs: TC-ETL-001 → TC-ETL-010
"""

import pytest
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src"))

from etl.transform import (
    normalize_column_names,
    cast_types,
    drop_duplicates,
    handle_nulls,
    add_ingestion_metadata,
    aggregate_sales,
)


# ---------------------------------------------------------------------------
# TC-ETL-001 | Normalize column names
# ---------------------------------------------------------------------------
def test_normalize_column_names(spark):
    """Column names should be lowercased with spaces replaced by underscores."""
    df = spark.createDataFrame([Row(**{"Product ID": "P1", "Sale Amount": 100.0})])
    result = normalize_column_names(df)
    assert "product_id" in result.columns
    assert "sale_amount" in result.columns


# ---------------------------------------------------------------------------
# TC-ETL-002 | Cast types — valid conversion
# ---------------------------------------------------------------------------
def test_cast_types_valid(spark):
    """Amount column should be cast from string to double without data loss."""
    df = spark.createDataFrame([("P1", "99.5"), ("P2", "200.0")], ["product_id", "amount"])
    result = cast_types(df, {"amount": "double"})
    assert result.schema["amount"].dataType == DoubleType()
    assert result.filter(result.amount == 99.5).count() == 1


# ---------------------------------------------------------------------------
# TC-ETL-003 | Cast types — invalid value becomes null
# ---------------------------------------------------------------------------
def test_cast_types_invalid_becomes_null(spark):
    """Non-numeric strings should become NULL when cast to double."""
    df = spark.createDataFrame([("P1", "NOT_A_NUMBER")], ["product_id", "amount"])
    result = cast_types(df, {"amount": "double"})
    from pyspark.sql import functions as F
    assert result.filter(F.col("amount").isNull()).count() == 1


# ---------------------------------------------------------------------------
# TC-ETL-004 | Drop duplicates — exact duplicates
# ---------------------------------------------------------------------------
def test_drop_duplicates_removes_exact(spark):
    """Exact duplicate rows must be removed, keeping one copy."""
    data = [("P1", 100.0), ("P1", 100.0), ("P2", 50.0)]
    df = spark.createDataFrame(data, ["product_id", "amount"])
    result = drop_duplicates(df)
    assert result.count() == 2


# ---------------------------------------------------------------------------
# TC-ETL-005 | Drop duplicates — subset key
# ---------------------------------------------------------------------------
def test_drop_duplicates_subset(spark):
    """Deduplication on subset should keep only first occurrence per key."""
    data = [("P1", 100.0), ("P1", 200.0), ("P2", 50.0)]
    df = spark.createDataFrame(data, ["product_id", "amount"])
    result = drop_duplicates(df, subset=["product_id"])
    assert result.count() == 2


# ---------------------------------------------------------------------------
# TC-ETL-006 | Handle nulls — drop strategy
# ---------------------------------------------------------------------------
def test_handle_nulls_drop(spark):
    """Rows with NULL in a 'drop' column must be removed."""
    data = [("P1", 100.0), (None, 50.0), ("P3", 30.0)]
    df = spark.createDataFrame(data, ["product_id", "amount"])
    result = handle_nulls(df, {"product_id": "drop"})
    assert result.count() == 2
    from pyspark.sql import functions as F
    assert result.filter(F.col("product_id").isNull()).count() == 0


# ---------------------------------------------------------------------------
# TC-ETL-007 | Handle nulls — fill with constant
# ---------------------------------------------------------------------------
def test_handle_nulls_fill_constant(spark):
    """Rows with NULL should be filled with the provided constant."""
    data = [("P1", None), ("P2", 50.0)]
    df = spark.createDataFrame(data, ["product_id", "amount"])
    result = handle_nulls(df, {"amount": 0.0})
    assert result.filter(result.amount == 0.0).count() == 1


# ---------------------------------------------------------------------------
# TC-ETL-008 | Metadata columns are added
# ---------------------------------------------------------------------------
def test_add_ingestion_metadata(spark):
    """DataFrame should gain _ingestion_ts and _source columns."""
    df = spark.createDataFrame([("P1", 100.0)], ["product_id", "amount"])
    result = add_ingestion_metadata(df, source="test_source")
    assert "_ingestion_ts" in result.columns
    assert "_source" in result.columns
    assert result.filter(result._source == "test_source").count() == 1


# ---------------------------------------------------------------------------
# TC-ETL-009 | Aggregate sales — correct grouping
# ---------------------------------------------------------------------------
def test_aggregate_sales_grouping(spark):
    """Sales must be grouped by date/product/region with correct sums."""
    data = [
        ("2024-01-01", "P1", "NORTH", 100.0),
        ("2024-01-01", "P1", "NORTH", 200.0),
        ("2024-01-01", "P2", "SOUTH", 50.0),
    ]
    df = spark.createDataFrame(data, ["date", "product_id", "region", "amount"])
    result = aggregate_sales(df)
    row = result.filter((result.product_id == "P1") & (result.region == "NORTH")).first()
    assert row["total_amount"] == 300.0
    assert row["transaction_count"] == 2


# ---------------------------------------------------------------------------
# TC-ETL-010 | Aggregate sales — empty DataFrame
# ---------------------------------------------------------------------------
def test_aggregate_sales_empty(spark):
    """Aggregation on an empty DataFrame must return an empty result."""
    schema = StructType([
        StructField("date", StringType()),
        StructField("product_id", StringType()),
        StructField("region", StringType()),
        StructField("amount", DoubleType()),
    ])
    df = spark.createDataFrame([], schema)
    result = aggregate_sales(df)
    assert result.count() == 0
