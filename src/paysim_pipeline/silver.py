"""Silver-layer cleansing and enrichment functions."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from paysim_pipeline.schemas import BRONZE_REQUIRED_COLUMNS
from paysim_pipeline.validation import validate_required_columns


VALID_TRANSACTION_TYPES = [
    "CASH_IN",
    "CASH_OUT",
    "DEBIT",
    "PAYMENT",
    "TRANSFER",
]


def select_valid_transactions(
    bronze_dataframe: DataFrame,
) -> DataFrame:
    """Apply the core validity rules for PaySim transactions."""

    validate_required_columns(
        dataframe=bronze_dataframe,
        required_columns=BRONZE_REQUIRED_COLUMNS,
        dataframe_name="bronze_dataframe",
    )

    return bronze_dataframe.filter(
        F.col("step").isNotNull()
        & (F.col("step") > 0)
        & F.col("transaction_type").isin(
            *VALID_TRANSACTION_TYPES
        )
        & F.col("amount").isNotNull()
        & (F.col("amount") >= 0)
        & F.col("origin_account").isNotNull()
        & F.col("destination_account").isNotNull()
        & F.col("is_fraud").isin(0, 1)
        & F.col("is_flagged_fraud").isin(0, 1)
    )


def add_time_features(
    dataframe: DataFrame,
) -> DataFrame:
    """Derive day and hour features from the PaySim step."""

    return (
        dataframe
        .withColumn(
            "transaction_day",
            (
                F.floor(
                    (F.col("step") - F.lit(1))
                    / F.lit(24)
                )
                + F.lit(1)
            ).cast("integer"),
        )
        .withColumn(
            "transaction_hour",
            F.pmod(
                F.col("step") - F.lit(1),
                F.lit(24),
            ).cast("integer"),
        )
    )


def add_balance_features(
    dataframe: DataFrame,
) -> DataFrame:
    """Create balance-difference and balance-error fields."""

    return (
        dataframe
        .withColumn(
            "origin_balance_change",
            F.round(
                F.col("origin_old_balance")
                - F.col("origin_new_balance"),
                2,
            ),
        )
        .withColumn(
            "destination_balance_change",
            F.round(
                F.col("destination_new_balance")
                - F.col("destination_old_balance"),
                2,
            ),
        )
        .withColumn(
            "origin_balance_error",
            F.round(
                F.col("origin_old_balance")
                - F.col("amount")
                - F.col("origin_new_balance"),
                2,
            ),
        )
        .withColumn(
            "destination_balance_error",
            F.round(
                F.col("destination_old_balance")
                + F.col("amount")
                - F.col("destination_new_balance"),
                2,
            ),
        )
    )


def add_transaction_indicators(
    dataframe: DataFrame,
    high_value_threshold: float,
) -> DataFrame:
    """Add reusable monitoring and segmentation indicators."""

    return (
        dataframe
        .withColumn(
            "is_high_value_transaction",
            F.when(
                F.col("amount")
                >= F.lit(high_value_threshold),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "is_zero_amount_transaction",
            F.when(
                F.col("amount") == 0,
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "is_origin_merchant",
            F.when(
                F.col("origin_account").startswith("M"),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "is_destination_merchant",
            F.when(
                F.col("destination_account").startswith("M"),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
    )


def build_silver_transactions(
    bronze_dataframe: DataFrame,
    high_value_threshold: float,
) -> DataFrame:
    """Run the complete Silver transformation."""

    valid_dataframe = select_valid_transactions(
        bronze_dataframe=bronze_dataframe,
    )

    time_enriched_dataframe = add_time_features(
        dataframe=valid_dataframe,
    )

    balance_enriched_dataframe = add_balance_features(
        dataframe=time_enriched_dataframe,
    )

    return add_transaction_indicators(
        dataframe=balance_enriched_dataframe,
        high_value_threshold=high_value_threshold,
    )
