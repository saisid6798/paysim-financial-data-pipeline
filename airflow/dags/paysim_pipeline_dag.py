"""Airflow DAG for the PaySim data pipeline."""

from __future__ import annotations

import os
import subprocess
import sys

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from airflow.sdk import dag, task


PROJECT_ROOT = Path(
    "/opt/airflow/project"
)

RAW_INPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "PS_20174392719_1491204439457_log.csv"
)

SUMMARY_OUTPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "summary_exports"
)

AUDIT_OUTPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "pipeline_audit"
)


EXPECTED_SUMMARY_FILES = {
    "daily_transaction_summary.csv",
    "daily_type_summary.csv",
    "fraud_monitoring.csv",
    "gold_table_registry.csv",
    "high_value_summary.csv",
    "hourly_fraud_summary.csv",
    "transaction_type_summary.csv",
}


@dag(
    dag_id="paysim_financial_data_pipeline",
    description=(
        "Run and validate the modular "
        "PaySim PySpark pipeline."
    ),
    schedule=None,
    start_date=datetime(
        2026,
        1,
        1,
        tzinfo=timezone.utc,
    ),
    catchup=False,
    max_active_runs=1,
    tags=[
        "paysim",
        "pyspark",
        "fraud",
        "data-engineering",
    ],
)
def paysim_pipeline_dag():
    """Define the PaySim orchestration workflow."""

    @task
    def validate_input() -> str:
        """Confirm that the raw dataset is available."""

        if not RAW_INPUT_PATH.exists():
            raise FileNotFoundError(
                "PaySim input file does not exist: "
                f"{RAW_INPUT_PATH}"
            )

        file_size_bytes = (
            RAW_INPUT_PATH.stat().st_size
        )

        if file_size_bytes <= 0:
            raise ValueError(
                "PaySim input file is empty."
            )

        print(
            "Input file:",
            RAW_INPUT_PATH,
        )

        print(
            "Input size in bytes:",
            file_size_bytes,
        )

        return str(RAW_INPUT_PATH)

    @task
    def run_pipeline(
        input_path: str,
    ) -> dict:
        """Execute the pipeline command-line runner."""

        command = [
            sys.executable,
            "-m",
            "paysim_pipeline.main",
            "--input",
            input_path,
            "--spark-master",
            "local[4]",
            "--driver-memory",
            "6g",
            "--shuffle-partitions",
            "64",
            "--high-value-threshold",
            "200000",
            "--approx-distinct-rsd",
            "0.05",
            "--clear-summary-outputs",
            "--log-level",
            "INFO",
        ]

        environment = os.environ.copy()

        environment["PYTHONPATH"] = str(
            PROJECT_ROOT / "src"
        )

        environment["PYSPARK_PYTHON"] = (
            sys.executable
        )

        environment[
            "PYSPARK_DRIVER_PYTHON"
        ] = sys.executable

        print(
            "Executing command:",
            " ".join(command),
        )

        completed_process = subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            env=environment,
            check=False,
        )

        if completed_process.returncode != 0:
            raise RuntimeError(
                "PaySim pipeline failed with "
                f"exit code "
                f"{completed_process.returncode}."
            )

        return {
            "exit_code": (
                completed_process.returncode
            ),
            "input_path": input_path,
        }

    @task
    def validate_outputs(
        pipeline_result: dict,
    ) -> dict:
        """Validate expected Gold CSV outputs."""

        if pipeline_result["exit_code"] != 0:
            raise RuntimeError(
                "Pipeline did not complete "
                "successfully."
            )

        actual_files = {
            path.name
            for path
            in SUMMARY_OUTPUT_PATH.glob(
                "*.csv"
            )
        }

        missing_files = (
            EXPECTED_SUMMARY_FILES
            - actual_files
        )

        if missing_files:
            raise FileNotFoundError(
                "Expected Gold outputs are "
                f"missing: {sorted(missing_files)}"
            )

        empty_files = [
            file_name
            for file_name
            in EXPECTED_SUMMARY_FILES
            if (
                SUMMARY_OUTPUT_PATH
                / file_name
            ).stat().st_size == 0
        ]

        if empty_files:
            raise ValueError(
                "Gold output files are empty: "
                f"{sorted(empty_files)}"
            )

        print(
            "Validated Gold output files:",
            sorted(
                EXPECTED_SUMMARY_FILES
            ),
        )

        return {
            "validated_file_count": len(
                EXPECTED_SUMMARY_FILES
            ),
            "output_directory": str(
                SUMMARY_OUTPUT_PATH
            ),
        }

    @task
    def validate_pipeline_summary(
        output_result: dict,
    ) -> dict:
        """Validate the newest pipeline summary."""

        summary_paths = sorted(
            AUDIT_OUTPUT_PATH.glob(
                "pipeline_summary_*.csv"
            ),
            key=lambda path: (
                path.stat().st_mtime
            ),
            reverse=True,
        )

        if not summary_paths:
            raise FileNotFoundError(
                "No pipeline summary file "
                "was generated."
            )

        latest_summary_path = (
            summary_paths[0]
        )

        summary_df = pd.read_csv(
            latest_summary_path
        )

        if summary_df.empty:
            raise ValueError(
                "Pipeline summary is empty."
            )

        pipeline_status = str(
            summary_df.loc[
                0,
                "pipeline_status",
            ]
        )

        reconciliation_status = str(
            summary_df.loc[
                0,
                (
                    "overall_"
                    "reconciliation_status"
                ),
            ]
        )

        if pipeline_status != "SUCCESS":
            raise RuntimeError(
                "Pipeline summary status is "
                f"{pipeline_status!r}."
            )

        if reconciliation_status != "PASS":
            raise RuntimeError(
                "Pipeline reconciliation status "
                f"is {reconciliation_status!r}."
            )

        print(
            "Latest summary:",
            latest_summary_path,
        )

        print(
            "Pipeline status:",
            pipeline_status,
        )

        print(
            "Reconciliation status:",
            reconciliation_status,
        )

        return {
            "pipeline_status": (
                pipeline_status
            ),
            "reconciliation_status": (
                reconciliation_status
            ),
            "validated_file_count": (
                output_result[
                    "validated_file_count"
                ]
            ),
            "summary_path": str(
                latest_summary_path
            ),
        }

    validated_input = validate_input()

    pipeline_result = run_pipeline(
        validated_input
    )

    validated_outputs = validate_outputs(
        pipeline_result
    )

    validate_pipeline_summary(
        validated_outputs
    )


paysim_pipeline_dag()