"""
Schema Validator — wip
"""

from dataclasses import dataclass, field
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


@dataclass
class ValidationResult:
    passed: bool = True
    errors: List[str] = field(default_factory=list)

    def fail(self, message: str) -> None:
        self.passed = False
        self.errors.append(message)

    def __str__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        lines = [f"[{status}] Schema Validation"]
        for err in self.errors:
            lines.append(f"  ✗ {err}")
        return "\n".join(lines)


def validate_schema(df: DataFrame, expected_schema: StructType) -> ValidationResult:
    result = ValidationResult()
    # pas sûre que simpleString() soit stable entre versions Spark — à surveiller
    actual_fields = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    expected_fields = {f.name: f.dataType.simpleString() for f in expected_schema.fields}

    # première passe : colonnes manquantes
    for col_name in expected_fields:
        if col_name not in actual_fields:
            result.fail(f"Missing column: '{col_name}'")

    # deuxième passe : types
    for col_name, expected_type in expected_fields.items():
        if col_name in actual_fields:
            if actual_fields[col_name] != expected_type:
                result.fail(f"Type mismatch on '{col_name}': expected {expected_type}, got {actual_fields[col_name]}")

    extra = set(actual_fields) - set(expected_fields)
    if extra:
        result.fail(f"Unexpected columns: {extra}")
    return result


def validate_not_null(df: DataFrame, columns: List[str]) -> ValidationResult:
    result = ValidationResult()
    from pyspark.sql import functions as F
    for col_name in columns:
        null_count = df.filter(F.col(col_name).isNull()).count()
        if null_count > 0:
            result.fail(f"Column '{col_name}' has {null_count} NULL value(s)")
    return result


def validate_row_count(df: DataFrame, min_rows: int = 1, max_rows: int = None) -> ValidationResult:
    result = ValidationResult()
    count = df.count()
    if count < min_rows:
        result.fail(f"Row count {count} is below minimum {min_rows}")
    if max_rows is not None and count > max_rows:
        result.fail(f"Row count {count} exceeds maximum {max_rows}")
    return result


def validate_uniqueness(df: DataFrame, subset: List[str]) -> ValidationResult:
    result = ValidationResult()
    total = df.count()
    distinct = df.dropDuplicates(subset).count()
    if total != distinct:
        result.fail(f"Duplicate rows detected on {subset}: {total - distinct} duplicate(s)")
    return result
