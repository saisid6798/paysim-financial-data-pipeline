"""Tests for audit and output utilities."""

import pandas as pd

from paysim_pipeline.audit import (
    combine_audit_records,
    create_audit_record,
)

from paysim_pipeline.io_utils import (
    clear_csv_outputs,
    export_small_dataframe_to_csv,
)


def test_create_audit_record(spark):
    """Audit record should describe its DataFrame."""

    dataframe = spark.createDataFrame(
        [
            (1, "A"),
            (2, "B"),
        ],
        ["id", "value"],
    )

    audit_df = create_audit_record(
        spark=spark,
        dataframe=dataframe,
        pipeline_run_id="run_001",
        pipeline_stage="TEST",
        table_name="test_table",
        execution_time_seconds=1.5,
        reconciliation_status="PASS",
    )

    row = audit_df.first()

    assert row.pipeline_run_id == "run_001"
    assert row.pipeline_stage == "TEST"
    assert row.table_name == "test_table"
    assert row.row_count == 2
    assert row.column_count == 2
    assert row.reconciliation_status == "PASS"


def test_combine_audit_records(spark):
    """Multiple audit records should union."""

    first_df = create_audit_record(
        spark=spark,
        dataframe=spark.range(2),
        pipeline_run_id="run_001",
        pipeline_stage="BRONZE",
        table_name="bronze_test",
        execution_time_seconds=1.0,
    )

    second_df = create_audit_record(
        spark=spark,
        dataframe=spark.range(3),
        pipeline_run_id="run_001",
        pipeline_stage="SILVER",
        table_name="silver_test",
        execution_time_seconds=2.0,
    )

    combined_df = combine_audit_records(
        [first_df, second_df]
    )

    assert combined_df.count() == 2


def test_export_small_dataframe_to_csv(
    spark,
    tmp_path,
):
    """Small Spark tables should export to CSV."""

    dataframe = spark.createDataFrame(
        [
            (1, "A"),
            (2, "B"),
        ],
        ["id", "value"],
    )

    output_path = (
        export_small_dataframe_to_csv(
            dataframe=dataframe,
            output_path=tmp_path,
            file_name="test_output.csv",
        )
    )

    assert output_path.exists()

    exported_df = pd.read_csv(
        output_path
    )

    assert len(exported_df) == 2
    assert list(exported_df.columns) == [
        "id",
        "value",
    ]


def test_clear_csv_outputs(tmp_path):
    """CSV cleanup should preserve other files."""

    first_csv = tmp_path / "first.csv"
    second_csv = tmp_path / "second.csv"
    text_file = tmp_path / "notes.txt"

    first_csv.write_text(
        "id\n1\n",
        encoding="utf-8",
    )

    second_csv.write_text(
        "id\n2\n",
        encoding="utf-8",
    )

    text_file.write_text(
        "retain me",
        encoding="utf-8",
    )

    deleted_files = clear_csv_outputs(
        output_path=tmp_path
    )

    assert len(deleted_files) == 2
    assert not first_csv.exists()
    assert not second_csv.exists()
    assert text_file.exists()
