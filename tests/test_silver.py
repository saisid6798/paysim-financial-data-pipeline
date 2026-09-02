"""Tests for Silver-layer transformations."""

from paysim_pipeline.silver import (
    build_silver_transactions,
)


def test_silver_preserves_valid_rows(
    bronze_df,
):
    """All valid test transactions should remain."""

    silver_df = build_silver_transactions(
        bronze_dataframe=bronze_df,
        high_value_threshold=200000.0,
    )

    assert silver_df.count() == 4


def test_negative_amount_is_rejected(
    spark,
    bronze_df,
):
    """Transactions with negative amounts are invalid."""

    invalid_row = (
        3,
        "PAYMENT",
        -10.0,
        "C010",
        100.0,
        110.0,
        "M010",
        0.0,
        0.0,
        0,
        0,
        "test_run_001",
    )

    invalid_df = spark.createDataFrame(
        [invalid_row],
        schema=bronze_df.schema,
    )

    combined_df = bronze_df.unionByName(
        invalid_df
    )

    silver_df = build_silver_transactions(
        bronze_dataframe=combined_df,
        high_value_threshold=200000.0,
    )

    assert combined_df.count() == 5
    assert silver_df.count() == 4


def test_time_features(silver_df):
    """Steps should map to correct day and hour."""

    time_rows = {
        row.step: (
            row.transaction_day,
            row.transaction_hour,
        )
        for row in (
            silver_df
            .select(
                "step",
                "transaction_day",
                "transaction_hour",
            )
            .collect()
        )
    }

    assert time_rows[1] == (1, 0)
    assert time_rows[2] == (1, 1)
    assert time_rows[25] == (2, 0)
    assert time_rows[26] == (2, 1)


def test_high_value_indicator(silver_df):
    """Only the 300,000 transaction is high value."""

    high_value_rows = (
        silver_df
        .filter(
            "is_high_value_transaction = 1"
        )
        .select("amount")
        .collect()
    )

    assert len(high_value_rows) == 1
    assert high_value_rows[0].amount == 300000.0


def test_origin_balance_error(silver_df):
    """Balanced origin transactions have zero error."""

    errors = [
        row.origin_balance_error
        for row in (
            silver_df
            .select(
                "origin_balance_error"
            )
            .collect()
        )
    ]

    assert all(
        error == 0.0
        for error in errors
    )


def test_destination_merchant_indicator(
    silver_df,
):
    """M-prefixed destinations are merchants."""

    payment_row = (
        silver_df
        .filter("step = 1")
        .select(
            "is_destination_merchant"
        )
        .first()
    )

    assert (
        payment_row
        .is_destination_merchant
        == 1
    )
