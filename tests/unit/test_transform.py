import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import pytest
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from etl.transform import normalize_column_names, cast_types, drop_duplicates


def test_normalize_removes_spaces(spark):
    df = spark.createDataFrame([("a", 1)], ["Col Name", "Other Col"])
    result = normalize_column_names(df)
    assert "col_name" in result.columns
    assert "other_col" in result.columns


def test_normalize_lowercases(spark):
    df = spark.createDataFrame([("a",)], ["AMOUNT"])
    result = normalize_column_names(df)
    assert "amount" in result.columns


def test_cast_types_converts_to_double(spark):
    df = spark.createDataFrame([("100",)], ["amount"])
    result = cast_types(df, {"amount": "double"})
    assert result.schema["amount"].dataType.simpleString() == "double"


def test_drop_duplicates_removes_exact(spark):
    data = [("P001", "NORTH", 100.0), ("P001", "NORTH", 100.0), ("P002", "SOUTH", 200.0)]
    df = spark.createDataFrame(data, ["product_id", "region", "amount"])
    result = drop_duplicates(df)
    assert result.count() == 2
