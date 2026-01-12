"""
ETL Module — Load
Writes transformed DataFrames to target storage (Parquet, Delta, CSV).
"""

from pyspark.sql import DataFrame
import logging

logger = logging.getLogger(__name__)


def load_parquet(df: DataFrame, path: str, mode: str = "overwrite", partition_by: list = None) -> None:
    """Write DataFrame to Parquet format."""
    logger.info(f"Writing {df.count()} rows to Parquet: {path}")
    writer = df.write.mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(path)
    logger.info("Parquet write complete.")


def load_csv(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    """Write DataFrame to CSV format (single file via coalesce)."""
    logger.info(f"Writing to CSV: {path}")
    df.coalesce(1).write.mode(mode).option("header", "true").csv(path)
    logger.info("CSV write complete.")


def load_delta(df: DataFrame, path: str, mode: str = "overwrite", partition_by: list = None) -> None:
    """Write DataFrame to Delta Lake format (requires delta library)."""
    logger.info(f"Writing to Delta: {path}")
    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)
    logger.info("Delta write complete.")


def load_jdbc(df: DataFrame, url: str, table: str, user: str, password: str, mode: str = "overwrite") -> None:
    """Write DataFrame to a JDBC-compatible database (PostgreSQL, MySQL…)."""
    logger.info(f"Writing to JDBC table: {table}")
    (
        df.write
          .mode(mode)
          .option("driver", "org.postgresql.Driver")
          .jdbc(url=url, table=table, properties={"user": user, "password": password})
    )
    logger.info("JDBC write complete.")
