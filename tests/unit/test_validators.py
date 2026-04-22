"""
Unit Tests — Schema & Business Rule Validators
XRAY IDs: TC-VAL-001 → TC-VAL-010
"""

import pytest
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src"))

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from validators.schema_validator import (
    validate_schema,
    validate_not_null,
    validate_row_count,
    validate_uniqueness,
)
from validators.business_rules import (
    validate_amount_positive,
    validate_date_range,
    validate_aggregate_totals,
    validate_referential_integrity,
)


EXPECTED_SCHEMA = StructType([
    StructField("product_id", StringType()),
    StructField("region", StringType()),
    StructField("total_amount", DoubleType()),
])


# ---------------------------------------------------------------------------
# TC-VAL-001 | Schema validation passes on correct DataFrame
# ---------------------------------------------------------------------------
def test_validate_schema_pass(spark):
    df = spark.createDataFrame([("P1", "NORTH", 100.0)], EXPECTED_SCHEMA)
    result = validate_schema(df, EXPECTED_SCHEMA)
    assert result.passed


# ---------------------------------------------------------------------------
# TC-VAL-002 | Schema validation fails on missing column
# ---------------------------------------------------------------------------
def test_validate_schema_missing_column(spark):
    df = spark.createDataFrame([("P1", 100.0)], ["product_id", "total_amount"])
    result = validate_schema(df, EXPECTED_SCHEMA)
    assert not result.passed
    assert any("region" in e for e in result.errors)


# ---------------------------------------------------------------------------
# TC-VAL-003 | Schema validation fails on type mismatch
# ---------------------------------------------------------------------------
def test_validate_schema_type_mismatch(spark):
    wrong_schema = StructType([
        StructField("product_id", StringType()),
        StructField("region", StringType()),
        StructField("total_amount", StringType()),   # should be DoubleType
    ])
    df = spark.createDataFrame([("P1", "NORTH", "100.0")], wrong_schema)
    result = validate_schema(df, EXPECTED_SCHEMA)
    assert not result.passed
    assert any("total_amount" in e for e in result.errors)


# ---------------------------------------------------------------------------
# TC-VAL-004 | Not-null check passes on clean data
# ---------------------------------------------------------------------------
def test_validate_not_null_pass(spark):
    df = spark.createDataFrame([("P1", 100.0)], ["product_id", "amount"])
    result = validate_not_null(df, ["product_id", "amount"])
    assert result.passed


# ---------------------------------------------------------------------------
# TC-VAL-005 | Not-null check fails when NULL exists
# ---------------------------------------------------------------------------
def test_validate_not_null_fail(spark):
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    schema = StructType([
        StructField("product_id", StringType()),
        StructField("amount", DoubleType()),
    ])
    df = spark.createDataFrame([(None, 100.0)], schema)
    result = validate_not_null(df, ["product_id"])
    assert not result.passed


# ---------------------------------------------------------------------------
# TC-VAL-006 | Row count within range passes
# ---------------------------------------------------------------------------
def test_validate_row_count_pass(spark):
    df = spark.createDataFrame([("P1",), ("P2",), ("P3",)], ["product_id"])
    result = validate_row_count(df, min_rows=1, max_rows=10)
    assert result.passed


# ---------------------------------------------------------------------------
# TC-VAL-007 | Row count below minimum fails
# ---------------------------------------------------------------------------
def test_validate_row_count_fail_empty(spark):
    df = spark.createDataFrame([], StructType([StructField("product_id", StringType())]))
    result = validate_row_count(df, min_rows=1)
    assert not result.passed


# ---------------------------------------------------------------------------
# TC-VAL-008 | Uniqueness check passes on distinct keys
# ---------------------------------------------------------------------------
def test_validate_uniqueness_pass(spark):
    df = spark.createDataFrame([("P1",), ("P2",), ("P3",)], ["product_id"])
    result = validate_uniqueness(df, subset=["product_id"])
    assert result.passed


# ---------------------------------------------------------------------------
# TC-VAL-009 | Uniqueness check fails on duplicate key
# ---------------------------------------------------------------------------
def test_validate_uniqueness_fail(spark):
    df = spark.createDataFrame([("P1",), ("P1",), ("P2",)], ["product_id"])
    result = validate_uniqueness(df, subset=["product_id"])
    assert not result.passed


# ---------------------------------------------------------------------------
# TC-VAL-010 | Amount positive check fails on negative value
# ---------------------------------------------------------------------------
def test_validate_amount_positive_fail(spark):
    df = spark.createDataFrame([("P1", -50.0), ("P2", 100.0)], ["product_id", "amount"])
    result = validate_amount_positive(df, col="amount")
    assert not result.passed
    assert "1 negative" in result.errors[0]

# TC-VAL-011 — edge case: empty dataframe should return zero rows
def test_validate_row_count_empty(spark):
    schema = StructType([StructField("id", StringType())])
    empty_df = spark.createDataFrame([], schema)
    result = validate_row_count(empty_df, min_rows=1)
    assert result.passed is False
