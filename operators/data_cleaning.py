"""DataCleaningOperator applies cleaning rules to records."""
import csv
import time
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Tuple

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

try:
    from prometheus_client import Counter, Gauge, Histogram, REGISTRY
except Exception:  # pragma: no cover
    Counter = None
    Gauge = None
    Histogram = None
    REGISTRY = None


def _collector(ctor, name: str, desc: str):
    if not ctor or not REGISTRY:
        return None
    existing = getattr(REGISTRY, "_names_to_collectors", {}).get(name)
    if existing:
        return existing
    try:
        return ctor(name, desc)
    except ValueError:
        return getattr(REGISTRY, "_names_to_collectors", {}).get(name)


RECORDS_PROCESSED = _collector(Counter, "cleaning_records_processed_total", "Records successfully cleaned")
RECORDS_FAILED = _collector(Counter, "cleaning_records_failed_total", "Records that failed cleaning")
PROCESSING_TIME = _collector(Histogram, "cleaning_processing_seconds", "Time spent cleaning batch")
DUPLICATE_RATE = _collector(Gauge, "cleaning_duplicate_rate", "Share of records flagged duplicate in last batch")


class DataCleaningOperator(BaseOperator):
    """Applies a cleaning function to a list of records and tracks audit info."""

    @apply_defaults
    def __init__(self, cleaning_fn: Callable[[Dict], Tuple[Dict, Dict]], records: Iterable[Dict] = None, file_path: str = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cleaning_fn = cleaning_fn
        self.records = list(records or [])
        self.file_path = file_path

    def execute(self, context):
        start = time.time()
        dataset = self.records
        if not dataset and self.file_path:
            dataset = self._load_file(self.file_path)
        cleaned: List[Dict] = []
        audit: List[Dict] = []
        for record in dataset:
            try:
                cleaned_record, audit_entry = self.cleaning_fn(record)
                cleaned.append(cleaned_record)
                audit.append(audit_entry)
                if RECORDS_PROCESSED:
                    RECORDS_PROCESSED.inc()
            except Exception as exc:  # noqa: B902
                if RECORDS_FAILED:
                    RECORDS_FAILED.inc()
                self.log.warning("Cleaning failed for record: %s", exc)
        duplicate_count = sum(1 for a in audit if a.get("is_duplicate") or a.get("duplicate"))
        if DUPLICATE_RATE and cleaned:
            DUPLICATE_RATE.set(duplicate_count / max(len(cleaned), 1))
        if PROCESSING_TIME:
            PROCESSING_TIME.observe(max(time.time() - start, 0))
        self.log.info("Cleaned %s records", len(cleaned))
        return {"cleaned": cleaned, "audit": audit}

    def _load_file(self, file_path: str) -> List[Dict]:
        try:
            import pandas as pd

            df = pd.read_csv(file_path)
            return df.to_dict(orient="records")
        except Exception:
            with open(Path(file_path), "r", newline="") as f:
                return list(csv.DictReader(f))
