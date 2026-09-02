"""Central configuration for the PaySim pipeline."""

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PipelineConfig:
    """Paths and runtime settings used by the pipeline."""

    project_root: Path
    raw_csv_path: Path

    application_name: str = "PaySimFinancialDataPipeline"
    spark_master: str = "local[4]"
    driver_memory: str = "8g"
    shuffle_partitions: int = 64
    high_value_threshold: float = 200_000.0
    approximate_distinct_rsd: float = 0.05

    @property
    def data_path(self) -> Path:
        return self.project_root / "data"

    @property
    def bronze_path(self) -> Path:
        return self.data_path / "bronze"

    @property
    def silver_path(self) -> Path:
        return self.data_path / "silver"

    @property
    def gold_path(self) -> Path:
        return self.data_path / "gold"

    @property
    def gold_summary_output_path(self) -> Path:
        return self.gold_path / "summary_exports"

    @property
    def audit_output_path(self) -> Path:
        return self.gold_path / "pipeline_audit"

    @property
    def spark_temp_path(self) -> Path:
        return self.project_root / "spark-temp"

    def create_directories(self) -> None:
        """Create pipeline output directories when they do not exist."""

        required_paths = [
            self.bronze_path,
            self.silver_path,
            self.gold_path,
            self.gold_summary_output_path,
            self.audit_output_path,
            self.spark_temp_path,
        ]

        for path in required_paths:
            path.mkdir(
                parents=True,
                exist_ok=True,
            )

    def validate(self) -> None:
        """Validate essential configuration values."""

        if not self.project_root.exists():
            raise FileNotFoundError(
                f"Project root does not exist: {self.project_root}"
            )

        if not self.raw_csv_path.exists():
            raise FileNotFoundError(
                f"Raw CSV does not exist: {self.raw_csv_path}"
            )

        if self.shuffle_partitions <= 0:
            raise ValueError(
                "shuffle_partitions must be greater than zero."
            )

        if not 0 < self.approximate_distinct_rsd <= 0.39:
            raise ValueError(
                "approximate_distinct_rsd must be between 0 and 0.39."
            )
