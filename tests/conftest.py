"""Shared PySpark fixtures for pipeline tests."""

import os
import sys
from pathlib import Path

import pytest

from pyspark.sql import SparkSession
from pyspark.sql import types as T


PROJECT_ROOT = Path(
    __file__
).resolve().parents[1]

SRC_PATH = PROJECT_ROOT / "src"

if str(SRC_PATH) not in sys.path:
    sys.path.insert(
        0,
        str(SRC_PATH),
    )


@pytest.fixture(scope="session")
def spark():
    """Create one Spark session for the test run."""

    python_executable = sys.executable

    os.environ["PYSPARK_PYTHON"] = (
        python_executable
    )
    os.environ["PYSPARK_DRIVER_PYTHON"] = (
        python_executable
    )

    session = (
        SparkSession.builder
        .appName("PaySimPipelineTests")
        .master("local[2]")
        .config(
            "spark.driver.memory",
            "2g",
        )
        .config(
            "spark.sql.shuffle.partitions",
            "4",
        )
        .config(
            "spark.default.parallelism",
            "2",
        )
        .config(
            "spark.sql.session.timeZone",
            "UTC",
        )
        .config(
            "spark.driver.host",
            "127.0.0.1",
        )
        .config(
            "spark.driver.bindAddress",
            "127.0.0.1",
        )
        .getOrCreate()
    )

    session.sparkContext.setLogLevel(
        "ERROR"
    )

    yield session

    session.catalog.clearCache()
    session.stop()


@pytest.fixture()
def bronze_schema():
    """Canonical Bronze schema for tests."""

    return T.StructType(
        [
            T.StructField(
                "step",
                T.IntegerType(),
                False,
            ),
            T.StructField(
                "transaction_type",
                T.StringType(),
                False,
            ),
            T.StructField(
                "amount",
                T.DoubleType(),
                False,
            ),
            T.StructField(
                "origin_account",
                T.StringType(),
                False,
            ),
            T.StructField(
                "origin_old_balance",
                T.DoubleType(),
                False,
            ),
            T.StructField(
                "origin_new_balance",
                T.DoubleType(),
                False,
            ),
            T.StructField(
                "destination_account",
                T.StringType(),
                False,
            ),
            T.StructField(
                "destination_old_balance",
                T.DoubleType(),
                False,
            ),
            T.StructField(
                "destination_new_balance",
                T.DoubleType(),
                False,
            ),
            T.StructField(
                "is_fraud",
                T.IntegerType(),
                False,
            ),
            T.StructField(
                "is_flagged_fraud",
                T.IntegerType(),
                False,
            ),
            T.StructField(
                "pipeline_run_id",
                T.StringType(),
                False,
            ),
        ]
    )


@pytest.fixture()
def bronze_df(
    spark,
    bronze_schema,
):
    """Create a small canonical Bronze dataset."""

    rows = [
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
            "test_run_001",
        ),
        (
            2,
            "TRANSFER",
            300000.0,
            "C002",
            500000.0,
            200000.0,
            "C003",
            0.0,
            300000.0,
            1,
            1,
            "test_run_001",
        ),
        (
            25,
            "CASH_OUT",
            500.0,
            "C002",
            200000.0,
            199500.0,
            "C004",
            0.0,
            500.0,
            1,
            0,
            "test_run_001",
        ),
        (
            26,
            "CASH_IN",
            0.0,
            "C005",
            1000.0,
            1000.0,
            "C006",
            100.0,
            100.0,
            0,
            0,
            "test_run_001",
        ),
    ]

    return spark.createDataFrame(
        rows,
        schema=bronze_schema,
    )


@pytest.fixture()
def silver_df(bronze_df):
    """Create Silver transactions for tests."""

    from paysim_pipeline.silver import (
        build_silver_transactions,
    )

    return build_silver_transactions(
        bronze_dataframe=bronze_df,
        high_value_threshold=200000.0,
    )
