"""
Business Rules Validator
Validates domain-level constraints: value ranges, referential integrity, totals.
"""

from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from validators.schema_validator import ValidationResult


def validate_amount_positive(df: DataFrame, col: str = "amount") -> ValidationResult:
    result = ValidationResult()
    neg = df.filter(F.col(col) < 0).count()
    if neg > 0:
        result.fail(f"{neg} negative value(s) found in column '{col}'")
    return result


def validate_date_range(df: DataFrame, col: str, min_date: str, max_date: str) -> ValidationResult:
    """Assert all dates fall within [min_date, max_date]."""
    result = ValidationResult()
    out = df.filter(
        (F.col(col) < F.lit(min_date)) | (F.col(col) > F.lit(max_date))
    ).count()
    if out > 0:
        result.fail(f"{out} row(s) with '{col}' outside [{min_date}, {max_date}]")
    return result


def validate_referential_integrity(
    df_fact: DataFrame,
    df_dim: DataFrame,
    fact_key: str,
    dim_key: str,
) -> ValidationResult:
    """Check that all fact keys exist in the dimension table (no orphan rows)."""
    result = ValidationResult()
    orphans = df_fact.join(df_dim, df_fact[fact_key] == df_dim[dim_key], "left_anti").count()
    if orphans > 0:
        result.fail(f"{orphans} orphan row(s): '{fact_key}' not found in dimension '{dim_key}'")
    return result


def validate_aggregate_totals(
    df_source: DataFrame,
    df_target: DataFrame,
    amount_col: str = "total_amount",
    tolerance: float = 0.01,
) -> ValidationResult:
    """
    Reconciliation check: sum of source amounts ≈ sum of target amounts.
    Tolerates floating-point drift up to `tolerance` ratio.
    """
    result = ValidationResult()
    src_total = df_source.select(F.sum(amount_col)).first()[0] or 0.0
    tgt_total = df_target.select(F.sum(amount_col)).first()[0] or 0.0

    if src_total == 0:
        result.fail("Source total is zero — cannot reconcile.")
        return result

    drift = abs(src_total - tgt_total) / src_total
    if drift > tolerance:
        result.fail(
            f"Amount mismatch: source={src_total:.2f}, target={tgt_total:.2f}, "
            f"drift={drift:.4%} > tolerance={tolerance:.4%}"
        )
    return result
