"""
ETL Pipeline — Orchestrator
Chains Extract → Transform → Load steps with logging and error handling.
"""

import logging
from pathlib import Path
from pyspark.sql import SparkSession

from etl.extract import create_spark_session, extract_csv
from etl.transform import (
    normalize_column_names,
    cast_types,
    drop_duplicates,
    handle_nulls,
    add_ingestion_metadata,
    aggregate_sales,
)
from etl.load import load_parquet

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

TYPE_MAP = {
    "amount": "double",
    "date": "timestamp",
    "product_id": "string",
    "region": "string",
}

NULL_STRATEGY = {
    "amount": "drop",
    "product_id": "drop",
    "region": "UNKNOWN",
}


def run_pipeline(raw_path: str, output_path: str, source_name: str = "sales_csv") -> None:
    spark = create_spark_session("SalesPipeline")
    try:
        # --- Extract ---
        df_raw = extract_csv(spark, raw_path)

        # --- Transform ---
        df = normalize_column_names(df_raw)
        df = cast_types(df, TYPE_MAP)
        df = drop_duplicates(df)
        df = handle_nulls(df, NULL_STRATEGY)
        df = add_ingestion_metadata(df, source=source_name)
        df_agg = aggregate_sales(df)

        # --- Load ---
        load_parquet(df_agg, output_path, partition_by=["date"])

        logger.info("Pipeline completed successfully.")
    except Exception as exc:
        logger.error(f"Pipeline failed: {exc}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    BASE = Path(__file__).resolve().parents[2]
    run_pipeline(
        raw_path=str(BASE / "data/raw/sales.csv"),
        output_path=str(BASE / "data/processed/sales_aggregated"),
    )
