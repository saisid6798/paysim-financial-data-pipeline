"""Reusable data-quality validation functions."""

from collections.abc import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def find_missing_columns(
    dataframe: DataFrame,
    required_columns: Sequence[str],
) -> list[str]:
    """Return required columns absent from a DataFrame."""

    available_columns = set(dataframe.columns)

    return [
        column
        for column in required_columns
        if column not in available_columns
    ]


def validate_required_columns(
    dataframe: DataFrame,
    required_columns: Sequence[str],
    dataframe_name: str,
) -> None:
    """Raise an error when required columns are absent."""

    missing_columns = find_missing_columns(
        dataframe=dataframe,
        required_columns=required_columns,
    )

    if missing_columns:
        raise ValueError(
            f"{dataframe_name} is missing required columns: "
            f"{missing_columns}"
        )


def build_null_profile(
    dataframe: DataFrame,
) -> DataFrame:
    """Return one row containing null counts for all columns."""

    expressions = [
        F.sum(
            F.when(
                F.col(column).isNull(),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias(column)
        for column in dataframe.columns
    ]

    return dataframe.agg(*expressions)


def count_duplicate_transactions(
    dataframe: DataFrame,
    key_columns: Sequence[str],
) -> int:
    """Count duplicate rows based on a proposed business key."""

    return (
        dataframe
        .groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )


def validate_binary_column(
    dataframe: DataFrame,
    column_name: str,
) -> DataFrame:
    """Return invalid values found in a binary indicator column."""

    return (
        dataframe
        .filter(
            F.col(column_name).isNotNull()
            & ~F.col(column_name).isin(0, 1)
        )
        .select(column_name)
        .distinct()
    )


def reconciliation_result(
    source_count: int,
    target_count: int,
) -> dict:
    """Build a reusable row-count reconciliation result."""

    return {
        "source_row_count": source_count,
        "target_row_count": target_count,
        "row_count_difference": target_count - source_count,
        "reconciliation_status": (
            "PASS"
            if source_count == target_count
            else "FAIL"
        ),
    }
