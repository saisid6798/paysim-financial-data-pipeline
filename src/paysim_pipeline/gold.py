"""Gold analytical-table construction functions."""

from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F

from paysim_pipeline.schemas import SILVER_REQUIRED_COLUMNS
from paysim_pipeline.validation import validate_required_columns


def _safe_percentage(
    numerator_column: str,
    denominator_column: str,
    precision: int = 6,
):
    """Return a divide-by-zero-safe percentage expression."""

    return F.when(
        F.col(denominator_column) > 0,
        F.round(
            F.col(numerator_column)
            / F.col(denominator_column)
            * F.lit(100.0),
            precision,
        ),
    ).otherwise(F.lit(0.0))


def create_daily_transaction_summary(
    silver_dataframe: DataFrame,
    approximate_distinct_rsd: float = 0.05,
) -> DataFrame:
    """Create one row per transaction day."""

    basic_summary = (
        silver_dataframe
        .groupBy("transaction_day")
        .agg(
            F.count("*").alias("transaction_count"),
            F.round(F.sum("amount"), 2).alias(
                "total_transaction_amount"
            ),
            F.round(F.avg("amount"), 2).alias(
                "average_transaction_amount"
            ),
            F.round(F.min("amount"), 2).alias(
                "minimum_transaction_amount"
            ),
            F.round(F.max("amount"), 2).alias(
                "maximum_transaction_amount"
            ),
            F.sum("is_fraud").alias("fraud_count"),
            F.round(
                F.sum(
                    F.when(
                        F.col("is_fraud") == 1,
                        F.col("amount"),
                    ).otherwise(F.lit(0.0))
                ),
                2,
            ).alias("fraud_amount"),
            F.sum("is_flagged_fraud").alias(
                "flagged_fraud_count"
            ),
            F.sum("is_high_value_transaction").alias(
                "high_value_transaction_count"
            ),
        )
    )

    account_summary = (
        silver_dataframe
        .groupBy("transaction_day")
        .agg(
            F.approx_count_distinct(
                "origin_account",
                approximate_distinct_rsd,
            ).alias(
                "estimated_unique_origin_accounts"
            ),
            F.approx_count_distinct(
                "destination_account",
                approximate_distinct_rsd,
            ).alias(
                "estimated_unique_destination_accounts"
            ),
        )
    )

    return (
        basic_summary
        .join(
            account_summary,
            on="transaction_day",
            how="left",
        )
        .withColumn(
            "fraud_rate_pct",
            _safe_percentage(
                "fraud_count",
                "transaction_count",
            ),
        )
        .withColumn(
            "fraud_amount_pct",
            _safe_percentage(
                "fraud_amount",
                "total_transaction_amount",
            ),
        )
        .orderBy("transaction_day")
    )


def create_daily_type_summary(
    silver_dataframe: DataFrame,
) -> DataFrame:
    """Create one row per day and transaction type."""

    return (
        silver_dataframe
        .groupBy(
            "transaction_day",
            "transaction_type",
        )
        .agg(
            F.count("*").alias("transaction_count"),
            F.round(F.sum("amount"), 2).alias(
                "total_transaction_amount"
            ),
            F.round(F.avg("amount"), 2).alias(
                "average_transaction_amount"
            ),
            F.sum("is_fraud").alias("fraud_count"),
            F.round(
                F.sum(
                    F.when(
                        F.col("is_fraud") == 1,
                        F.col("amount"),
                    ).otherwise(F.lit(0.0))
                ),
                2,
            ).alias("fraud_amount"),
        )
        .withColumn(
            "fraud_rate_pct",
            _safe_percentage(
                "fraud_count",
                "transaction_count",
            ),
        )
        .orderBy(
            "transaction_day",
            "transaction_type",
        )
    )


def create_hourly_fraud_summary(
    silver_dataframe: DataFrame,
) -> DataFrame:
    """Create one row per PaySim hourly step."""

    return (
        silver_dataframe
        .groupBy(
            F.col("step").alias("hourly_step")
        )
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("is_fraud").alias("fraud_count"),
            F.round(F.sum("amount"), 2).alias(
                "total_transaction_amount"
            ),
            F.round(
                F.sum(
                    F.when(
                        F.col("is_fraud") == 1,
                        F.col("amount"),
                    ).otherwise(F.lit(0.0))
                ),
                2,
            ).alias("fraud_amount"),
        )
        .withColumn(
            "fraud_rate_pct",
            _safe_percentage(
                "fraud_count",
                "transaction_count",
            ),
        )
        .orderBy("hourly_step")
    )


def create_transaction_type_summary(
    silver_dataframe: DataFrame,
) -> DataFrame:
    """Create one row per transaction type."""

    return (
        silver_dataframe
        .groupBy("transaction_type")
        .agg(
            F.count("*").alias("transaction_count"),
            F.round(F.sum("amount"), 2).alias(
                "total_transaction_amount"
            ),
            F.round(F.avg("amount"), 2).alias(
                "average_transaction_amount"
            ),
            F.sum("is_fraud").alias("fraud_count"),
            F.sum("is_flagged_fraud").alias(
                "flagged_fraud_count"
            ),
            F.sum("is_high_value_transaction").alias(
                "high_value_transaction_count"
            ),
        )
        .withColumn(
            "fraud_rate_pct",
            _safe_percentage(
                "fraud_count",
                "transaction_count",
            ),
        )
        .orderBy("transaction_type")
    )


def create_origin_account_summary(
    silver_dataframe: DataFrame,
) -> DataFrame:
    """Create one row per origin account."""

    return (
        silver_dataframe
        .groupBy("origin_account")
        .agg(
            F.count("*").alias(
                "origin_transaction_count"
            ),
            F.round(F.sum("amount"), 2).alias(
                "origin_total_amount"
            ),
            F.round(F.avg("amount"), 2).alias(
                "origin_average_amount"
            ),
            F.sum("is_fraud").alias(
                "origin_fraud_count"
            ),
            F.max("step").alias(
                "latest_origin_transaction_step"
            ),
        )
    )


def create_destination_account_summary(
    silver_dataframe: DataFrame,
) -> DataFrame:
    """Create one row per destination account."""

    return (
        silver_dataframe
        .groupBy("destination_account")
        .agg(
            F.count("*").alias(
                "destination_transaction_count"
            ),
            F.round(F.sum("amount"), 2).alias(
                "destination_total_amount"
            ),
            F.round(F.avg("amount"), 2).alias(
                "destination_average_amount"
            ),
            F.sum("is_fraud").alias(
                "destination_fraud_count"
            ),
            F.max("step").alias(
                "latest_destination_transaction_step"
            ),
        )
    )


def create_high_value_summary(
    silver_dataframe: DataFrame,
) -> DataFrame:
    """Create one row per day and type for high-value transactions."""

    return (
        silver_dataframe
        .filter(
            F.col("is_high_value_transaction") == 1
        )
        .groupBy(
            "transaction_day",
            "transaction_type",
        )
        .agg(
            F.count("*").alias(
                "high_value_transaction_count"
            ),
            F.round(F.sum("amount"), 2).alias(
                "high_value_total_amount"
            ),
            F.round(F.avg("amount"), 2).alias(
                "high_value_average_amount"
            ),
            F.sum("is_fraud").alias(
                "high_value_fraud_count"
            ),
        )
        .orderBy(
            "transaction_day",
            "transaction_type",
        )
    )


def create_fraud_monitoring_table(
    silver_dataframe: DataFrame,
) -> DataFrame:
    """Return transaction-level fraudulent records."""

    return (
        silver_dataframe
        .filter(F.col("is_fraud") == 1)
        .select(
            "step",
            "transaction_day",
            "transaction_hour",
            "transaction_type",
            "amount",
            "origin_account",
            "destination_account",
            "is_fraud",
            "is_flagged_fraud",
            "is_high_value_transaction",
            "pipeline_run_id",
        )
        .orderBy(
            "step",
            "origin_account",
        )
    )


def create_fraud_feature_table(
    silver_dataframe: DataFrame,
) -> DataFrame:
    """Create leakage-aware transaction features."""

    origin_history_window = (
        Window
        .partitionBy("origin_account")
        .orderBy("step")
        .rowsBetween(
            Window.unboundedPreceding,
            -1,
        )
    )

    return (
        silver_dataframe
        .withColumn(
            "prior_origin_transaction_count",
            F.count("*").over(
                origin_history_window
            ),
        )
        .withColumn(
            "prior_origin_average_amount",
            F.round(
                F.avg("amount").over(
                    origin_history_window
                ),
                2,
            ),
        )
        .withColumn(
            "prior_origin_maximum_amount",
            F.round(
                F.max("amount").over(
                    origin_history_window
                ),
                2,
            ),
        )
        .select(
            "step",
            "transaction_day",
            "transaction_hour",
            "transaction_type",
            "amount",
            "origin_account",
            "destination_account",
            "is_high_value_transaction",
            "is_destination_merchant",
            "prior_origin_transaction_count",
            "prior_origin_average_amount",
            "prior_origin_maximum_amount",
            "is_fraud",
        )
    )


def build_gold_tables(
    silver_dataframe: DataFrame,
    approximate_distinct_rsd: float = 0.05,
) -> dict[str, DataFrame]:
    """Create and return all Gold tables."""

    validate_required_columns(
        dataframe=silver_dataframe,
        required_columns=SILVER_REQUIRED_COLUMNS,
        dataframe_name="silver_dataframe",
    )

    return {
        "daily_transaction_summary": (
            create_daily_transaction_summary(
                silver_dataframe,
                approximate_distinct_rsd,
            )
        ),
        "daily_type_summary": (
            create_daily_type_summary(
                silver_dataframe
            )
        ),
        "hourly_fraud_summary": (
            create_hourly_fraud_summary(
                silver_dataframe
            )
        ),
        "transaction_type_summary": (
            create_transaction_type_summary(
                silver_dataframe
            )
        ),
        "origin_account_summary": (
            create_origin_account_summary(
                silver_dataframe
            )
        ),
        "destination_account_summary": (
            create_destination_account_summary(
                silver_dataframe
            )
        ),
        "high_value_summary": (
            create_high_value_summary(
                silver_dataframe
            )
        ),
        "fraud_monitoring": (
            create_fraud_monitoring_table(
                silver_dataframe
            )
        ),
        "fraud_feature": (
            create_fraud_feature_table(
                silver_dataframe
            )
        ),
    }
