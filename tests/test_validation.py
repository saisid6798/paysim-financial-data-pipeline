"""Tests for data-quality validation functions."""

import pytest

from paysim_pipeline.validation import (
    build_null_profile,
    count_duplicate_transactions,
    find_missing_columns,
    reconciliation_result,
    validate_binary_column,
    validate_required_columns,
)


def test_find_missing_columns(spark):
    """Missing columns should be returned."""

    dataframe = spark.createDataFrame(
        [(1, "PAYMENT")],
        ["step", "transaction_type"],
    )

    missing_columns = find_missing_columns(
        dataframe=dataframe,
        required_columns=[
            "step",
            "transaction_type",
            "amount",
        ],
    )

    assert missing_columns == ["amount"]


def test_validate_required_columns_passes(
    spark,
):
    """Validation should pass when columns exist."""

    dataframe = spark.createDataFrame(
        [(1, 100.0)],
        ["step", "amount"],
    )

    validate_required_columns(
        dataframe=dataframe,
        required_columns=[
            "step",
            "amount",
        ],
        dataframe_name="test_dataframe",
    )


def test_validate_required_columns_fails(
    spark,
):
    """Validation should fail when a column is absent."""

    dataframe = spark.createDataFrame(
        [(1,)],
        ["step"],
    )

    with pytest.raises(
        ValueError,
        match="missing required columns",
    ):
        validate_required_columns(
            dataframe=dataframe,
            required_columns=[
                "step",
                "amount",
            ],
            dataframe_name="test_dataframe",
        )


def test_null_profile(spark):
    """Null profile should count null values."""

    dataframe = spark.createDataFrame(
        [
            (1, "A"),
            (2, None),
            (None, "C"),
        ],
        ["number", "letter"],
    )

    result = build_null_profile(
        dataframe
    ).first()

    assert result.number == 1
    assert result.letter == 1


def test_duplicate_count(spark):
    """Duplicate business keys should be counted."""

    dataframe = spark.createDataFrame(
        [
            (1, "A"),
            (1, "A"),
            (2, "B"),
        ],
        ["step", "account"],
    )

    duplicate_group_count = (
        count_duplicate_transactions(
            dataframe=dataframe,
            key_columns=[
                "step",
                "account",
            ],
        )
    )

    assert duplicate_group_count == 1


def test_binary_column_validation(spark):
    """Invalid binary values should be returned."""

    dataframe = spark.createDataFrame(
        [
            (0,),
            (1,),
            (2,),
            (None,),
        ],
        ["indicator"],
    )

    invalid_values = {
        row.indicator
        for row in (
            validate_binary_column(
                dataframe=dataframe,
                column_name="indicator",
            )
            .collect()
        )
    }

    assert invalid_values == {2}


def test_successful_reconciliation():
    """Equal counts should produce PASS."""

    result = reconciliation_result(
        source_count=100,
        target_count=100,
    )

    assert (
        result["reconciliation_status"]
        == "PASS"
    )
    assert result["row_count_difference"] == 0


def test_failed_reconciliation():
    """Different counts should produce FAIL."""

    result = reconciliation_result(
        source_count=100,
        target_count=95,
    )

    assert (
        result["reconciliation_status"]
        == "FAIL"
    )
    assert (
        result["row_count_difference"]
        == -5
    )
