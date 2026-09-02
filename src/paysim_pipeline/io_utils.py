"""Reusable pipeline output utilities."""

from pathlib import Path

from pyspark.sql import DataFrame


def export_small_dataframe_to_csv(
    dataframe: DataFrame,
    output_path: Path,
    file_name: str,
    index: bool = False,
) -> Path:
    """Export a small Spark DataFrame as one local CSV file."""

    output_path.mkdir(
        parents=True,
        exist_ok=True,
    )

    final_path = output_path / file_name

    pandas_dataframe = dataframe.toPandas()

    pandas_dataframe.to_csv(
        final_path,
        index=index,
    )

    return final_path


def write_dataframe_to_parquet(
    dataframe: DataFrame,
    output_path: Path,
    mode: str = "overwrite",
    partition_columns: list[str] | None = None,
) -> None:
    """Write a large Spark DataFrame to Parquet."""

    writer = dataframe.write.mode(mode)

    if partition_columns:
        writer = writer.partitionBy(
            *partition_columns
        )

    writer.parquet(
        str(output_path)
    )


def clear_csv_outputs(
    output_path: Path,
) -> list[Path]:
    """Delete existing CSV outputs and return their paths."""

    output_path.mkdir(
        parents=True,
        exist_ok=True,
    )

    deleted_files = []

    for file_path in output_path.glob("*.csv"):
        file_path.unlink()
        deleted_files.append(file_path)

    return deleted_files
