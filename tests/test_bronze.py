"""Tests for Bronze-layer functions."""

from pyspark.sql import types as T

from paysim_pipeline.bronze import (
    add_bronze_metadata,
    standardize_bronze_columns,
)


def test_standardize_bronze_columns(
    spark,
):
    """Raw columns should receive canonical names."""

    raw_schema = T.StructType(
        [
            T.StructField(
                "step",
                T.IntegerType(),
                False,
            ),
            T.StructField(
                "type",
                T.StringType(),
                False,
            ),
            T.StructField(
                "amount",
                T.DoubleType(),
                False,
            ),
            T.StructField(
                "nameOrig",
                T.StringType(),
                False,
            ),
            T.StructField(
                "oldbalanceOrg",
                T.DoubleType(),
                False,
            ),
            T.StructField(
                "newbalanceOrig",
                T.DoubleType(),
                False,
            ),
            T.StructField(
                "nameDest",
                T.StringType(),
                False,
            ),
            T.StructField(
                "oldbalanceDest",
                T.DoubleType(),
                False,
            ),
            T.StructField(
                "newbalanceDest",
                T.DoubleType(),
                False,
            ),
            T.StructField(
                "isFraud",
                T.IntegerType(),
                False,
            ),
            T.StructField(
                "isFlaggedFraud",
                T.IntegerType(),
                False,
            ),
        ]
    )

    raw_df = spark.createDataFrame(
        [
            (
                1,
                "PAYMENT",
                100.0,
                "C001",
                1000.0,
                900.0,
                "M001",
                0.0,
                0.0,
                0,
                0,
            )
        ],
        schema=raw_schema,
    )

    result_df = standardize_bronze_columns(
        raw_dataframe=raw_df
    )

    assert "transaction_type" in (
        result_df.columns
    )
    assert "origin_account" in (
        result_df.columns
    )
    assert "destination_account" in (
        result_df.columns
    )
    assert "is_fraud" in result_df.columns
    assert "type" not in result_df.columns
    assert "nameOrig" not in result_df.columns


def test_add_bronze_metadata(
    spark,
):
    """Bronze records should contain audit metadata."""

    source_df = spark.createDataFrame(
        [(1,)],
        ["step"],
    )

    result_df = add_bronze_metadata(
        dataframe=source_df,
        source_file_name="test.csv",
        pipeline_run_id="run_001",
    )

    row = result_df.first()

    assert row.source_file_name == "test.csv"
    assert row.pipeline_run_id == "run_001"
    assert (
        row.bronze_ingestion_timestamp
        is not None
    )
