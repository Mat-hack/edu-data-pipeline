"""Custom operator to validate input files."""
import os
import csv
from typing import Iterable, List, Optional

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class FileValidationOperator(BaseOperator):
    """Validates file structure before processing."""

    @apply_defaults
    def __init__(self, file_path: str, required_columns: Optional[Iterable[str]] = None, max_size_mb: int = 25, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_path = file_path
        self.required_columns = list(required_columns or [])
        self.max_size_mb = max_size_mb

    def execute(self, context):
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"File not found: {self.file_path}")
        size_mb = os.path.getsize(self.file_path) / (1024 * 1024)
        if size_mb > self.max_size_mb:
            raise ValueError(f"File too large: {size_mb:.2f} MB > {self.max_size_mb} MB")
        if not self.required_columns:
            self.log.info("No required columns specified, skipping header validation")
            return {"file_path": self.file_path, "size_mb": size_mb, "columns": []}
        with open(self.file_path, "r", newline="") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                raise ValueError("File has no header row")
            missing = [col for col in self.required_columns if col not in reader.fieldnames]
            if missing:
                raise ValueError(f"Missing columns: {missing}")
        return {"file_path": self.file_path, "size_mb": size_mb, "columns": reader.fieldnames}
