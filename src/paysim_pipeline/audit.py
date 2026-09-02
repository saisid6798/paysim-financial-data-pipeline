"""Pipeline audit-record utilities."""

from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T


AUDIT_SCHEMA = T.StructType(
    [
        T.StructField(
            "pipeline_run_id",
            T.StringType(),
            False,
        ),
        T.StructField(
            "pipeline_stage",
            T.StringType(),
            False,
        ),
        T.StructField(
            "table_name",
            T.StringType(),
            False,
        ),
        T.StructField(
            "row_count",
            T.LongType(),
            False,
        ),
        T.StructField(
            "column_count",
            T.IntegerType(),
            False,
        ),
        T.StructField(
            "reconciliation_status",
            T.StringType(),
            False,
        ),
        T.StructField(
            "execution_time_seconds",
            T.DoubleType(),
            False,
        ),
        T.StructField(
            "audit_timestamp_utc",
            T.TimestampType(),
            False,
        ),
    ]
)


def create_audit_record(
    spark: SparkSession,
    dataframe: DataFrame,
    pipeline_run_id: str,
    pipeline_stage: str,
    table_name: str,
    execution_time_seconds: float,
    reconciliation_status: str = "PASS",
) -> DataFrame:
    """Create a one-row Spark audit DataFrame."""

    audit_row = [
        (
            pipeline_run_id,
            pipeline_stage,
            table_name,
            dataframe.count(),
            len(dataframe.columns),
            reconciliation_status,
            float(execution_time_seconds),
            datetime.now(timezone.utc).replace(
                tzinfo=None
            ),
        )
    ]

    return spark.createDataFrame(
        audit_row,
        schema=AUDIT_SCHEMA,
    )


def combine_audit_records(
    audit_dataframes: list[DataFrame],
) -> DataFrame:
    """Union multiple audit DataFrames."""

    if not audit_dataframes:
        raise ValueError(
            "At least one audit DataFrame is required."
        )

    combined_dataframe = audit_dataframes[0]

    for audit_dataframe in audit_dataframes[1:]:
        combined_dataframe = (
            combined_dataframe.unionByName(
                audit_dataframe
            )
        )

    return combined_dataframe
