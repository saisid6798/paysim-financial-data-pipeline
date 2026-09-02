"""Bronze-layer ingestion functions."""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from paysim_pipeline.schemas import RAW_TRANSACTION_SCHEMA


RAW_TO_CANONICAL_COLUMN_MAP = {
    "type": "transaction_type",
    "nameOrig": "origin_account",
    "oldbalanceOrg": "origin_old_balance",
    "newbalanceOrig": "origin_new_balance",
    "nameDest": "destination_account",
    "oldbalanceDest": "destination_old_balance",
    "newbalanceDest": "destination_new_balance",
    "isFraud": "is_fraud",
    "isFlaggedFraud": "is_flagged_fraud",
}


def read_raw_transactions(
    spark: SparkSession,
    raw_csv_path: Path,
) -> DataFrame:
    """Read the PaySim CSV using an explicit schema."""

    return (
        spark.read
        .option("header", True)
        .option("mode", "FAILFAST")
        .schema(RAW_TRANSACTION_SCHEMA)
        .csv(str(raw_csv_path))
    )


def standardize_bronze_columns(
    raw_dataframe: DataFrame,
) -> DataFrame:
    """Rename raw PaySim columns to canonical pipeline names."""

    bronze_dataframe = raw_dataframe

    for source_column, target_column in (
        RAW_TO_CANONICAL_COLUMN_MAP.items()
    ):
        bronze_dataframe = bronze_dataframe.withColumnRenamed(
            source_column,
            target_column,
        )

    return bronze_dataframe


def add_bronze_metadata(
    dataframe: DataFrame,
    source_file_name: str,
    pipeline_run_id: str,
) -> DataFrame:
    """Add ingestion metadata to Bronze records."""

    return (
        dataframe
        .withColumn(
            "source_file_name",
            F.lit(source_file_name),
        )
        .withColumn(
            "pipeline_run_id",
            F.lit(pipeline_run_id),
        )
        .withColumn(
            "bronze_ingestion_timestamp",
            F.current_timestamp(),
        )
    )


def build_bronze_transactions(
    spark: SparkSession,
    raw_csv_path: Path,
    pipeline_run_id: str,
) -> DataFrame:
    """Run the complete Bronze ingestion transformation."""

    raw_dataframe = read_raw_transactions(
        spark=spark,
        raw_csv_path=raw_csv_path,
    )

    standardized_dataframe = standardize_bronze_columns(
        raw_dataframe=raw_dataframe,
    )

    return add_bronze_metadata(
        dataframe=standardized_dataframe,
        source_file_name=raw_csv_path.name,
        pipeline_run_id=pipeline_run_id,
    )
