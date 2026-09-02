"""Tests for Gold analytical tables."""

from paysim_pipeline.gold import (
    build_gold_tables,
    create_daily_transaction_summary,
    create_fraud_feature_table,
    create_fraud_monitoring_table,
    create_high_value_summary,
    create_transaction_type_summary,
)


def test_daily_summary_grain(silver_df):
    """Daily summary should have one row per day."""

    result_df = (
        create_daily_transaction_summary(
            silver_dataframe=silver_df,
            approximate_distinct_rsd=0.05,
        )
    )

    assert result_df.count() == 2

    days = {
        row.transaction_day
        for row in (
            result_df
            .select("transaction_day")
            .collect()
        )
    }

    assert days == {1, 2}


def test_daily_summary_reconciles(
    silver_df,
):
    """Daily transaction counts should reconcile."""

    result_df = (
        create_daily_transaction_summary(
            silver_dataframe=silver_df,
            approximate_distinct_rsd=0.05,
        )
    )

    reconciled_count = (
        result_df
        .groupBy()
        .sum("transaction_count")
        .first()[0]
    )

    assert reconciled_count == 4


def test_daily_fraud_counts(silver_df):
    """Daily fraud counts should equal Silver."""

    result_df = (
        create_daily_transaction_summary(
            silver_dataframe=silver_df,
            approximate_distinct_rsd=0.05,
        )
    )

    daily_fraud_count = (
        result_df
        .groupBy()
        .sum("fraud_count")
        .first()[0]
    )

    assert daily_fraud_count == 2


def test_transaction_type_summary(
    silver_df,
):
    """Each transaction type should appear once."""

    result_df = (
        create_transaction_type_summary(
            silver_dataframe=silver_df
        )
    )

    assert result_df.count() == 4

    reconciled_count = (
        result_df
        .groupBy()
        .sum("transaction_count")
        .first()[0]
    )

    assert reconciled_count == 4


def test_high_value_summary(silver_df):
    """Only one test transaction is high value."""

    result_df = create_high_value_summary(
        silver_dataframe=silver_df
    )

    assert result_df.count() == 1

    row = result_df.first()

    assert (
        row.high_value_transaction_count
        == 1
    )
    assert (
        row.high_value_total_amount
        == 300000.0
    )
    assert row.high_value_fraud_count == 1


def test_fraud_monitoring_table(
    silver_df,
):
    """Fraud monitoring contains fraud only."""

    result_df = (
        create_fraud_monitoring_table(
            silver_dataframe=silver_df
        )
    )

    assert result_df.count() == 2

    fraud_values = {
        row.is_fraud
        for row in (
            result_df
            .select("is_fraud")
            .collect()
        )
    }

    assert fraud_values == {1}


def test_fraud_features_use_prior_records(
    silver_df,
):
    """C002's second transaction sees one prior record."""

    feature_df = (
        create_fraud_feature_table(
            silver_dataframe=silver_df
        )
    )

    row = (
        feature_df
        .filter(
            "origin_account = 'C002' "
            "AND step = 25"
        )
        .first()
    )

    assert row is not None
    assert (
        row.prior_origin_transaction_count
        == 1
    )
    assert (
        row.prior_origin_average_amount
        == 300000.0
    )


def test_build_gold_table_registry(
    silver_df,
):
    """Gold builder should return all expected tables."""

    gold_tables = build_gold_tables(
        silver_dataframe=silver_df,
        approximate_distinct_rsd=0.05,
    )

    expected_tables = {
        "daily_transaction_summary",
        "daily_type_summary",
        "hourly_fraud_summary",
        "transaction_type_summary",
        "origin_account_summary",
        "destination_account_summary",
        "high_value_summary",
        "fraud_monitoring",
        "fraud_feature",
    }

    assert set(gold_tables) == expected_tables
