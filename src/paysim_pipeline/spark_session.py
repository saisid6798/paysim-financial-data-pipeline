"""Spark-session utilities."""

import os
import sys

from pyspark.sql import SparkSession

from paysim_pipeline.config import PipelineConfig


def create_spark_session(
    config: PipelineConfig,
) -> SparkSession:
    """Create and configure a local Spark session."""

    python_executable = sys.executable

    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

    config.spark_temp_path.mkdir(
        parents=True,
        exist_ok=True,
    )

    spark = (
        SparkSession.builder
        .appName(config.application_name)
        .master(config.spark_master)
        .config(
            "spark.driver.memory",
            config.driver_memory,
        )
        .config(
            "spark.sql.shuffle.partitions",
            str(config.shuffle_partitions),
        )
        .config(
            "spark.local.dir",
            str(config.spark_temp_path),
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

    spark.sparkContext.setLogLevel("WARN")

    return spark
