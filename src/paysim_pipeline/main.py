"""Command-line entry point for the PaySim pipeline."""

from __future__ import annotations

import argparse
import logging
import sys
import traceback
import uuid

from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter

import pandas as pd

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from paysim_pipeline.bronze import (
    build_bronze_transactions,
)
from paysim_pipeline.config import PipelineConfig
from paysim_pipeline.gold import (
    build_gold_tables,
)
from paysim_pipeline.io_utils import (
    clear_csv_outputs,
    export_small_dataframe_to_csv,
)
from paysim_pipeline.schemas import (
    BRONZE_REQUIRED_COLUMNS,
    SILVER_REQUIRED_COLUMNS,
)
from paysim_pipeline.silver import (
    build_silver_transactions,
)
from paysim_pipeline.spark_session import (
    create_spark_session,
)
from paysim_pipeline.validation import (
    validate_required_columns,
)


LOGGER = logging.getLogger(
    "paysim_pipeline"
)


SMALL_GOLD_TABLES = [
    "daily_transaction_summary",
    "daily_type_summary",
    "hourly_fraud_summary",
    "transaction_type_summary",
    "high_value_summary",
]


SKIPPED_LARGE_GOLD_TABLES = [
    "origin_account_summary",
    "destination_account_summary",
    "fraud_feature",
]


def get_project_root() -> Path:
    """Return the project root from this file."""

    return (
        Path(__file__)
        .resolve()
        .parents[2]
    )


def resolve_path(
    path_value: str,
    project_root: Path,
) -> Path:
    """Resolve an absolute or project-relative path."""

    path = Path(path_value)

    if not path.is_absolute():
        path = project_root / path

    return path.resolve()


def create_pipeline_run_id() -> str:
    """Generate a unique pipeline run identifier."""

    run_timestamp = datetime.now(
        timezone.utc
    ).strftime("%Y%m%d_%H%M%S")

    random_suffix = uuid.uuid4().hex[:8]

    return (
        f"{run_timestamp}_{random_suffix}"
    )


def configure_logging(
    project_root: Path,
    pipeline_run_id: str,
    log_level: str,
) -> Path:
    """Configure console and file logging."""

    log_directory = (
        project_root / "logs"
    )

    log_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    log_path = (
        log_directory
        / f"pipeline_{pipeline_run_id}.log"
    )

    numeric_log_level = getattr(
        logging,
        log_level.upper(),
        logging.INFO,
    )

    formatter = logging.Formatter(
        fmt=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(name)s | "
            "%(message)s"
        ),
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = (
        logging.StreamHandler(sys.stdout)
    )

    console_handler.setFormatter(
        formatter
    )

    file_handler = logging.FileHandler(
        log_path,
        encoding="utf-8",
    )

    file_handler.setFormatter(
        formatter
    )

    logging.basicConfig(
        level=numeric_log_level,
        handlers=[
            console_handler,
            file_handler,
        ],
        force=True,
    )

    return log_path


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(
        description=(
            "Execute the modular PaySim "
            "Bronze-Silver-Gold pipeline."
        )
    )

    parser.add_argument(
        "--input",
        default=(
            "data/raw/"
            "PS_20174392719_1491204439457_log.csv"
        ),
        help=(
            "Raw PaySim CSV path. Relative paths "
            "are resolved from the project root."
        ),
    )

    parser.add_argument(
        "--spark-master",
        default="local[4]",
        help=(
            "Spark master, such as local[4]."
        ),
    )

    parser.add_argument(
        "--driver-memory",
        default="8g",
        help=(
            "Spark driver memory, such as 4g or 8g."
        ),
    )

    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=64,
        help=(
            "Number of Spark SQL shuffle partitions."
        ),
    )

    parser.add_argument(
        "--high-value-threshold",
        type=float,
        default=200000.0,
        help=(
            "Amount threshold used to identify "
            "high-value transactions."
        ),
    )

    parser.add_argument(
        "--approx-distinct-rsd",
        type=float,
        default=0.05,
        help=(
            "Relative standard deviation for "
            "approximate distinct counts."
        ),
    )

    parser.add_argument(
        "--clear-summary-outputs",
        action="store_true",
        help=(
            "Delete existing summary CSV files "
            "before pipeline execution."
        ),
    )

    parser.add_argument(
        "--log-level",
        choices=[
            "DEBUG",
            "INFO",
            "WARNING",
            "ERROR",
        ],
        default="INFO",
        help="Application logging level.",
    )

    return parser.parse_args()


def create_audit_record(
    pipeline_run_id: str,
    pipeline_stage: str,
    table_name: str,
    row_count: int,
    column_count: int,
    execution_time_seconds: float,
    reconciliation_status: str,
) -> dict:
    """Create a Python audit record."""

    return {
        "pipeline_run_id": (
            pipeline_run_id
        ),
        "pipeline_stage": (
            pipeline_stage
        ),
        "table_name": table_name,
        "row_count": int(row_count),
        "column_count": int(
            column_count
        ),
        "reconciliation_status": (
            reconciliation_status
        ),
        "execution_time_seconds": round(
            execution_time_seconds,
            3,
        ),
        "audit_timestamp_utc": (
            datetime.now(
                timezone.utc
            ).isoformat()
        ),
    }


def export_audit_records(
    audit_records: list[dict],
    output_path: Path,
    pipeline_run_id: str,
) -> Path:
    """Export pipeline audit records."""

    output_path.mkdir(
        parents=True,
        exist_ok=True,
    )

    audit_path = (
        output_path
        / f"pipeline_audit_"
        f"{pipeline_run_id}.csv"
    )

    pd.DataFrame(
        audit_records
    ).to_csv(
        audit_path,
        index=False,
    )

    return audit_path


def export_pipeline_summary(
    summary: dict,
    output_path: Path,
    pipeline_run_id: str,
) -> Path:
    """Export the final pipeline summary."""

    output_path.mkdir(
        parents=True,
        exist_ok=True,
    )

    summary_path = (
        output_path
        / f"pipeline_summary_"
        f"{pipeline_run_id}.csv"
    )

    pd.DataFrame(
        [summary]
    ).to_csv(
        summary_path,
        index=False,
    )

    return summary_path


def run_pipeline(
    arguments: argparse.Namespace,
) -> int:
    """Execute the complete PaySim pipeline."""

    project_root = get_project_root()

    pipeline_run_id = (
        create_pipeline_run_id()
    )

    log_path = configure_logging(
        project_root=project_root,
        pipeline_run_id=pipeline_run_id,
        log_level=arguments.log_level,
    )

    run_started_at = datetime.now(
        timezone.utc
    )

    pipeline_timer = perf_counter()

    spark: SparkSession | None = None
    silver_df = None
    cached_gold_tables = []
    fraud_monitoring_df = None

    audit_records: list[dict] = []
    exported_files: list[Path] = []

    LOGGER.info(
        "Starting PaySim pipeline"
    )
    LOGGER.info(
        "Pipeline run ID: %s",
        pipeline_run_id,
    )
    LOGGER.info(
        "Project root: %s",
        project_root,
    )
    LOGGER.info(
        "Log file: %s",
        log_path,
    )

    try:
        raw_csv_path = resolve_path(
            path_value=arguments.input,
            project_root=project_root,
        )

        config = PipelineConfig(
            project_root=project_root,
            raw_csv_path=raw_csv_path,
            application_name=(
                "PaySimCommandLinePipeline"
            ),
            spark_master=(
                arguments.spark_master
            ),
            driver_memory=(
                arguments.driver_memory
            ),
            shuffle_partitions=(
                arguments.shuffle_partitions
            ),
            high_value_threshold=(
                arguments
                .high_value_threshold
            ),
            approximate_distinct_rsd=(
                arguments
                .approx_distinct_rsd
            ),
        )

        config.create_directories()
        config.validate()

        LOGGER.info(
            "Pipeline configuration validated"
        )
        LOGGER.info(
            "Input file: %s",
            config.raw_csv_path,
        )

        if arguments.clear_summary_outputs:
            deleted_files = (
                clear_csv_outputs(
                    config
                    .gold_summary_output_path
                )
            )

            LOGGER.info(
                "Deleted %s previous summary "
                "CSV files",
                len(deleted_files),
            )

        spark = create_spark_session(
            config=config
        )

        LOGGER.info(
            "Spark session created"
        )
        LOGGER.info(
            "Spark version: %s",
            spark.version,
        )
        LOGGER.info(
            "Spark master: %s",
            spark.sparkContext.master,
        )

        # ---------------------------------
        # Bronze
        # ---------------------------------

        bronze_timer = perf_counter()

        bronze_df = (
            build_bronze_transactions(
                spark=spark,
                raw_csv_path=(
                    config.raw_csv_path
                ),
                pipeline_run_id=(
                    pipeline_run_id
                ),
            )
        )

        validate_required_columns(
            dataframe=bronze_df,
            required_columns=(
                BRONZE_REQUIRED_COLUMNS
            ),
            dataframe_name="bronze_df",
        )

        bronze_row_count = (
            bronze_df.count()
        )

        bronze_seconds = (
            perf_counter() - bronze_timer
        )

        audit_records.append(
            create_audit_record(
                pipeline_run_id=(
                    pipeline_run_id
                ),
                pipeline_stage="BRONZE",
                table_name=(
                    "bronze_transactions"
                ),
                row_count=(
                    bronze_row_count
                ),
                column_count=len(
                    bronze_df.columns
                ),
                execution_time_seconds=(
                    bronze_seconds
                ),
                reconciliation_status=(
                    "PASS"
                ),
            )
        )

        LOGGER.info(
            "Bronze completed: %s rows "
            "in %.2f seconds",
            f"{bronze_row_count:,}",
            bronze_seconds,
        )

        # ---------------------------------
        # Silver
        # ---------------------------------

        silver_timer = perf_counter()

        silver_df = (
            build_silver_transactions(
                bronze_dataframe=bronze_df,
                high_value_threshold=(
                    config
                    .high_value_threshold
                ),
            )
            .persist(
                StorageLevel.DISK_ONLY
            )
        )

        validate_required_columns(
            dataframe=silver_df,
            required_columns=(
                SILVER_REQUIRED_COLUMNS
            ),
            dataframe_name="silver_df",
        )

        silver_row_count = (
            silver_df.count()
        )

        silver_seconds = (
            perf_counter() - silver_timer
        )

        bronze_to_silver_status = (
            "PASS"
            if (
                0
                <= silver_row_count
                <= bronze_row_count
            )
            else "FAIL"
        )

        audit_records.append(
            create_audit_record(
                pipeline_run_id=(
                    pipeline_run_id
                ),
                pipeline_stage="SILVER",
                table_name=(
                    "silver_transactions"
                ),
                row_count=(
                    silver_row_count
                ),
                column_count=len(
                    silver_df.columns
                ),
                execution_time_seconds=(
                    silver_seconds
                ),
                reconciliation_status=(
                    bronze_to_silver_status
                ),
            )
        )

        LOGGER.info(
            "Silver completed: %s rows "
            "in %.2f seconds",
            f"{silver_row_count:,}",
            silver_seconds,
        )

        LOGGER.info(
            "Bronze-to-Silver "
            "reconciliation: %s",
            bronze_to_silver_status,
        )

        # ---------------------------------
        # Gold
        # ---------------------------------

        gold_tables = build_gold_tables(
            silver_dataframe=silver_df,
            approximate_distinct_rsd=(
                config
                .approximate_distinct_rsd
            ),
        )

        gold_row_counts: dict[
            str,
            int,
        ] = {}

        gold_execution_times: dict[
            str,
            float,
        ] = {}

        for table_name in (
            SMALL_GOLD_TABLES
        ):
            LOGGER.info(
                "Executing Gold table: %s",
                table_name,
            )

            gold_timer = perf_counter()

            dataframe = (
                gold_tables[table_name]
                .persist(
                    StorageLevel
                    .MEMORY_AND_DISK
                )
            )

            gold_tables[table_name] = (
                dataframe
            )

            cached_gold_tables.append(
                dataframe
            )

            row_count = dataframe.count()

            execution_seconds = (
                perf_counter()
                - gold_timer
            )

            output_path = (
                export_small_dataframe_to_csv(
                    dataframe=dataframe,
                    output_path=(
                        config
                        .gold_summary_output_path
                    ),
                    file_name=(
                        f"{table_name}.csv"
                    ),
                )
            )

            exported_files.append(
                output_path
            )

            gold_row_counts[
                table_name
            ] = row_count

            gold_execution_times[
                table_name
            ] = execution_seconds

            LOGGER.info(
                "Gold table %s completed: "
                "%s rows in %.2f seconds",
                table_name,
                f"{row_count:,}",
                execution_seconds,
            )

        # ---------------------------------
        # Fraud-monitoring CSV
        # ---------------------------------

        fraud_timer = perf_counter()

        fraud_monitoring_df = (
            silver_df
            .filter(
                F.col("is_fraud") == 1
            )
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
                (
                    "is_high_value_"
                    "transaction"
                ),
                "pipeline_run_id",
            )
            .persist(
                StorageLevel
                .MEMORY_AND_DISK
            )
        )

        fraud_monitoring_count = (
            fraud_monitoring_df.count()
        )

        fraud_monitoring_path = (
            export_small_dataframe_to_csv(
                dataframe=(
                    fraud_monitoring_df
                ),
                output_path=(
                    config
                    .gold_summary_output_path
                ),
                file_name=(
                    "fraud_monitoring.csv"
                ),
            )
        )

        exported_files.append(
            fraud_monitoring_path
        )

        fraud_seconds = (
            perf_counter() - fraud_timer
        )

        # ---------------------------------
        # Reconciliation
        # ---------------------------------

        silver_fraud_count = (
            silver_df
            .agg(
                F.sum("is_fraud").alias(
                    "fraud_count"
                )
            )
            .first()["fraud_count"]
        )

        daily_reconciled_count = (
            gold_tables[
                "daily_transaction_summary"
            ]
            .agg(
                F.sum(
                    "transaction_count"
                ).alias(
                    "reconciled_count"
                )
            )
            .first()["reconciled_count"]
        )

        type_reconciled_count = (
            gold_tables[
                "transaction_type_summary"
            ]
            .agg(
                F.sum(
                    "transaction_count"
                ).alias(
                    "reconciled_count"
                )
            )
            .first()["reconciled_count"]
        )

        daily_fraud_count = (
            gold_tables[
                "daily_transaction_summary"
            ]
            .agg(
                F.sum(
                    "fraud_count"
                ).alias(
                    "fraud_count"
                )
            )
            .first()["fraud_count"]
        )

        daily_reconciliation_status = (
            "PASS"
            if (
                daily_reconciled_count
                == silver_row_count
            )
            else "FAIL"
        )

        type_reconciliation_status = (
            "PASS"
            if (
                type_reconciled_count
                == silver_row_count
            )
            else "FAIL"
        )

        fraud_reconciliation_status = (
            "PASS"
            if (
                daily_fraud_count
                == silver_fraud_count
                == fraud_monitoring_count
            )
            else "FAIL"
        )

        LOGGER.info(
            "Daily reconciliation: %s",
            daily_reconciliation_status,
        )
        LOGGER.info(
            "Transaction-type "
            "reconciliation: %s",
            type_reconciliation_status,
        )
        LOGGER.info(
            "Fraud reconciliation: %s",
            fraud_reconciliation_status,
        )

        # ---------------------------------
        # Gold audit records
        # ---------------------------------

        gold_status_map = {
            "daily_transaction_summary": (
                daily_reconciliation_status
            ),
            "daily_type_summary": (
                fraud_reconciliation_status
            ),
            "hourly_fraud_summary": (
                "PASS"
            ),
            "transaction_type_summary": (
                type_reconciliation_status
            ),
            "high_value_summary": "PASS",
        }

        for table_name in (
            SMALL_GOLD_TABLES
        ):
            audit_records.append(
                create_audit_record(
                    pipeline_run_id=(
                        pipeline_run_id
                    ),
                    pipeline_stage="GOLD",
                    table_name=table_name,
                    row_count=(
                        gold_row_counts[
                            table_name
                        ]
                    ),
                    column_count=len(
                        gold_tables[
                            table_name
                        ].columns
                    ),
                    execution_time_seconds=(
                        gold_execution_times[
                            table_name
                        ]
                    ),
                    reconciliation_status=(
                        gold_status_map[
                            table_name
                        ]
                    ),
                )
            )

        audit_records.append(
            create_audit_record(
                pipeline_run_id=(
                    pipeline_run_id
                ),
                pipeline_stage="GOLD",
                table_name=(
                    "fraud_monitoring"
                ),
                row_count=(
                    fraud_monitoring_count
                ),
                column_count=len(
                    fraud_monitoring_df.columns
                ),
                execution_time_seconds=(
                    fraud_seconds
                ),
                reconciliation_status=(
                    fraud_reconciliation_status
                ),
            )
        )

        # ---------------------------------
        # Gold registry
        # ---------------------------------

        registry_records = []

        for table_name, dataframe in (
            gold_tables.items()
        ):
            if table_name in (
                SMALL_GOLD_TABLES
            ):
                persistence_status = (
                    "CSV_EXPORTED"
                )
                row_count = (
                    gold_row_counts[
                        table_name
                    ]
                )
                persistence_format = "CSV"

            elif (
                table_name
                == "fraud_monitoring"
            ):
                persistence_status = (
                    "CSV_EXPORTED"
                )
                row_count = (
                    fraud_monitoring_count
                )
                persistence_format = "CSV"

            elif table_name in (
                SKIPPED_LARGE_GOLD_TABLES
            ):
                persistence_status = (
                    "DEFINED_NOT_PERSISTED"
                )
                row_count = None
                persistence_format = None

            else:
                persistence_status = (
                    "NOT_EXECUTED"
                )
                row_count = None
                persistence_format = None

            registry_records.append(
                {
                    "pipeline_run_id": (
                        pipeline_run_id
                    ),
                    "table_name": (
                        table_name
                    ),
                    "column_count": len(
                        dataframe.columns
                    ),
                    "row_count": (
                        row_count
                    ),
                    "persistence_format": (
                        persistence_format
                    ),
                    "persistence_status": (
                        persistence_status
                    ),
                }
            )

        registry_path = (
            config
            .gold_summary_output_path
            / "gold_table_registry.csv"
        )

        pd.DataFrame(
            registry_records
        ).sort_values(
            "table_name"
        ).to_csv(
            registry_path,
            index=False,
        )

        exported_files.append(
            registry_path
        )

        # ---------------------------------
        # Determine final status
        # ---------------------------------

        reconciliation_statuses = [
            bronze_to_silver_status,
            daily_reconciliation_status,
            type_reconciliation_status,
            fraud_reconciliation_status,
        ]

        pipeline_status = (
            "SUCCESS"
            if all(
                status == "PASS"
                for status
                in reconciliation_statuses
            )
            else (
                "COMPLETED_WITH_"
                "VALIDATION_FAILURE"
            )
        )

        audit_path = (
            export_audit_records(
                audit_records=(
                    audit_records
                ),
                output_path=(
                    config
                    .audit_output_path
                ),
                pipeline_run_id=(
                    pipeline_run_id
                ),
            )
        )

        run_completed_at = datetime.now(
            timezone.utc
        )

        total_seconds = (
            perf_counter()
            - pipeline_timer
        )

        summary = {
            "pipeline_run_id": (
                pipeline_run_id
            ),
            "pipeline_status": (
                pipeline_status
            ),
            "run_started_at_utc": (
                run_started_at.isoformat()
            ),
            "run_completed_at_utc": (
                run_completed_at.isoformat()
            ),
            "total_execution_seconds": round(
                total_seconds,
                3,
            ),
            "bronze_row_count": (
                bronze_row_count
            ),
            "silver_row_count": (
                silver_row_count
            ),
            "rejected_row_count": (
                bronze_row_count
                - silver_row_count
            ),
            "fraud_transaction_count": (
                silver_fraud_count
            ),
            "csv_files_exported": len(
                exported_files
            ),
            "large_gold_tables_skipped": (
                len(
                    SKIPPED_LARGE_GOLD_TABLES
                )
            ),
            "overall_reconciliation_status": (
                "PASS"
                if pipeline_status
                == "SUCCESS"
                else "FAIL"
            ),
        }

        summary_path = (
            export_pipeline_summary(
                summary=summary,
                output_path=(
                    config
                    .audit_output_path
                ),
                pipeline_run_id=(
                    pipeline_run_id
                ),
            )
        )

        LOGGER.info(
            "Audit exported: %s",
            audit_path,
        )
        LOGGER.info(
            "Summary exported: %s",
            summary_path,
        )
        LOGGER.info(
            "Pipeline status: %s",
            pipeline_status,
        )
        LOGGER.info(
            "Total execution time: "
            "%.2f seconds",
            total_seconds,
        )

        if pipeline_status != "SUCCESS":
            return 1

        return 0

    except Exception as exc:
        total_seconds = (
            perf_counter()
            - pipeline_timer
        )

        LOGGER.exception(
            "Pipeline failed: %s",
            exc,
        )

        failure_directory = (
            project_root
            / "data"
            / "gold"
            / "pipeline_audit"
        )

        failure_directory.mkdir(
            parents=True,
            exist_ok=True,
        )

        failure_path = (
            failure_directory
            / f"pipeline_failure_"
            f"{pipeline_run_id}.csv"
        )

        failure_record = {
            "pipeline_run_id           ": (
                pipeline_run_id
            ),
            "pipeline_status": "FAILED",
            "failure_timestamp_utc": (
                datetime.now(
                    timezone.utc
                ).isoformat()
            ),
            "execution_time_seconds": round(
                total_seconds,
                3,
            ),
            "exception_type": (
                type(exc).__name__
            ),
            "exception_message": str(exc),
            "traceback": (
                traceback.format_exc()
            ),
        }

        pd.DataFrame(
            [failure_record]
        ).to_csv(
            failure_path,
            index=False,
        )

        LOGGER.error(
            "Failure audit exported: %s",
            failure_path,
        )

        return 1

    finally:
        LOGGER.info(
            "Releasing pipeline resources"
        )

        if fraud_monitoring_df is not None:
            fraud_monitoring_df.unpersist(
                blocking=False
            )

        for dataframe in (
            cached_gold_tables
        ):
            dataframe.unpersist(
                blocking=False
            )

        if silver_df is not None:
            silver_df.unpersist(
                blocking=True
            )

        if spark is not None:
            try:
                spark.catalog.clearCache()
                spark.stop()

                LOGGER.info(
                    "Spark session stopped"
                )

            except Exception:
                LOGGER.exception(
                    "Error while stopping Spark"
                )


def main() -> None:
    """Run the CLI application."""

    arguments = parse_arguments()

    exit_code = run_pipeline(
        arguments=arguments
    )

    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()