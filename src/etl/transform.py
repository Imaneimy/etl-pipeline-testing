"""
ETL Module — Transform
transformations appliquées aux DataFrames bruts avant chargement.
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType

logger = logging.getLogger(__name__)


def normalize_column_names(df: DataFrame) -> DataFrame:
    # Orange Maroc: les colonnes venaient pas toujours dans le même ordre depuis SAP
    # et parfois avec des espaces ou des majuscules — on normalise tout ici
    return df.toDF(*[c.lower().replace(" ", "_") for c in df.columns])


def cast_types(df: DataFrame, type_map: dict) -> DataFrame:
    """
    Cast columns to specified types.
    type_map = {"col_name": "double", "other_col": "timestamp"}
    """
    # TODO: refactorer ça plus tard — trop verbeux pour ce que ça fait
    for col_name, col_type in type_map.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, df[col_name].cast(col_type))
            logger.debug(f"Cast {col_name} → {col_type}")
    return df


def drop_duplicates(df: DataFrame, subset: list = None) -> DataFrame:
    before = df.count()
    df = df.dropDuplicates(subset) if subset else df.dropDuplicates()
    removed = before - df.count()
    logger.info(f"Removed {removed} duplicate rows")
    return df


def handle_nulls(df: DataFrame, strategy: dict) -> DataFrame:
    """
    Fill or drop nulls per column.
    strategy = {"col": "drop"} or {"col": 0} or {"col": "mean"}

    NOTE: strategy="mean" plante si la colonne est de type string — limitation connue,
    à corriger plus tard (voir draft/notes.md)
    """
    for col_name, action in strategy.items():
        if col_name not in df.columns:
            continue
        if action == "drop":
            df = df.filter(F.col(col_name).isNotNull())
        elif action == "mean":
            # pas sûre que ce soit la meilleure approche mais ça marche pour les numériques
            mean_val = df.select(F.mean(col_name)).first()[0]
            df_temp = df.fillna({col_name: mean_val})
            df = df_temp
        else:
            df = df.fillna({col_name: action})
    return df


def add_ingestion_metadata(df: DataFrame, source: str) -> DataFrame:
    return (
        df.withColumn("_ingestion_ts", F.current_timestamp())
          .withColumn("_source", F.lit(source))
    )


def aggregate_sales(df: DataFrame) -> DataFrame:
    """
    agrégation journalière par produit et région.
    colonnes attendues : date, product_id, region, amount
    """
    return (
        df.groupBy("date", "product_id", "region")
          .agg(
              F.sum("amount").alias("total_amount"),
              F.count("*").alias("transaction_count"),
              F.avg("amount").alias("avg_amount"),
          )
          .orderBy("date", "product_id")
    )
