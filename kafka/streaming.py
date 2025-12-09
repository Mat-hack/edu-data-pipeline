"""Kafka streaming simulation classes."""
import json
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional

import psycopg2
from psycopg2.extras import execute_values

try:
    from prometheus_client import Counter, Histogram, start_http_server
except Exception:  # pragma: no cover
    Counter = None
    Histogram = None
    start_http_server = None

EVENTS_PRODUCED = Counter("kafka_events_produced_total", "Events produced") if Counter else None
EVENTS_CONSUMED = Counter("kafka_events_consumed_total", "Events consumed") if Counter else None
PROCESSING_LATENCY = Histogram("processor_latency_seconds", "Processing latency") if Histogram else None


@dataclass
class EventSimulatorProducer:
    """Reads CSV file and publishes records as stream."""

    publish_rate: int = 10  # events per second
    topic: str = "raw.student.events"

    def simulate_stream(self, file_path: str, producer) -> None:
        import csv

        with open(file_path, "r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                event = self._to_event(row)
                producer.send(self.topic, value=event)
                if EVENTS_PRODUCED:
                    EVENTS_PRODUCED.inc()
                time.sleep(1 / max(self.publish_rate, 1))

    def _to_event(self, row: Dict) -> Dict:
        return {
            **row,
            "event_time": datetime.now(timezone.utc).isoformat(),
            "source": "file-simulator",
        }


@dataclass
class StreamAggregator:
    """Maintains rolling aggregations for streaming analytics."""

    per_student: Dict[str, Dict] = field(default_factory=dict)
    window_events: List[Dict] = field(default_factory=list)

    def update(self, event: Dict) -> Dict:
        student_id = event.get("student_id")
        if not student_id:
            return {}
        metrics = self.per_student.setdefault(student_id, {"count": 0, "duration": 0, "errors": 0})
        metrics["count"] += 1
        metrics["duration"] += int(event.get("duration_seconds") or 0)
        if not event.get("is_valid", True):
            metrics["errors"] += 1
        self.window_events.append(event)
        self._trim_window()
        return metrics

    def _trim_window(self):
        # Placeholder for window management; in a real app use timestamps
        if len(self.window_events) > 1000:
            self.window_events = self.window_events[-1000:]


@dataclass
class RealTimeProcessor:
    """Consumes events, cleans them, detects anomalies, and forwards."""

    producer: object
    cleaned_topic: str = "processed.cleaned.events"
    enriched_topic: str = "processed.enriched.events"
    dlq_topic: str = "dlq.failed.events"
    aggregator: StreamAggregator = field(default_factory=StreamAggregator)
    metrics_port: int = int(os.environ.get("METRICS_PORT", "8001"))
    staging_writer: Optional["StagingProgressWriter"] = None

    def __post_init__(self):
        if start_http_server:
            try:
                start_http_server(self.metrics_port)
            except Exception:
                pass

    def process_event(self, event: Dict) -> Dict:
        if EVENTS_CONSUMED:
            EVENTS_CONSUMED.inc()
        timer = PROCESSING_LATENCY.time() if PROCESSING_LATENCY else None
        try:
            cleaned = self._clean_event(event)
            agg = self.aggregator.update(cleaned)
            anomaly = self._detect_anomaly(agg)
            payload = {**cleaned, "anomaly": anomaly}
            self.producer.send(self.cleaned_topic, value=payload)
            if anomaly:
                self.producer.send("alerts.anomalies", value=payload)
            if self.staging_writer:
                self.staging_writer.safe_write([payload])
            return payload
        except Exception as exc:  # noqa: B902
            self.producer.send(self.dlq_topic, value={"event": event, "error": str(exc)})
            return {"error": str(exc)}
        finally:
            if timer:
                timer.observe_duration()

    def _clean_event(self, event: Dict) -> Dict:
        cleaned = dict(event)
        cleaned["is_valid"] = True
        if cleaned.get("score"):
            try:
                cleaned["score"] = min(max(float(cleaned["score"]), 0), 100)
            except Exception:
                cleaned["is_valid"] = False
        return cleaned

    def _detect_anomaly(self, metrics: Dict) -> bool:
        error_rate = metrics.get("errors", 0) / max(metrics.get("count", 1), 1)
        return error_rate > 0.2


class StagingProgressWriter:
    """Persists cleaned streaming events into staging.stg_progress."""

    def __init__(self, table: str = "staging.stg_progress", batch_id: Optional[str] = None):
        self.table = table
        self.batch_id = batch_id or datetime.utcnow().isoformat()
        self.conn_str = self._pg_conn_str()

    def _pg_conn_str(self) -> str:
        raw = os.environ.get("PG_DSN") or os.environ.get("DATABASE_URL") or os.environ.get("AIRFLOW__CORE__SQL_ALCHEMY_CONN")
        if not raw:
            raise RuntimeError("Database connection string not set for streaming sink")
        if raw.startswith("postgresql+psycopg2"):
            raw = raw.replace("postgresql+psycopg2", "postgresql", 1)
        return raw

    def safe_write(self, events: Iterable[Dict]) -> int:
        try:
            return self.write(events)
        except Exception:
            return 0

    def write(self, events: Iterable[Dict]) -> int:
        rows = []
        cols = [
            "event_id",
            "student_id",
            "course_id",
            "event_type",
            "event_timestamp",
            "duration_seconds",
            "score",
            "module_id",
            "completion_percentage",
            "is_student_valid",
            "is_timestamp_valid",
            "is_score_valid",
            "is_duplicate",
            "quality_score",
            "batch_id",
        ]
        for ev in events:
            event_id = ev.get("event_id") or f"stream-{uuid.uuid4()}"
            ts_raw = ev.get("event_time") or ev.get("event_timestamp")
            ts = self._parse_ts(ts_raw)
            rows.append([
                event_id,
                ev.get("student_id"),
                ev.get("course_id"),
                ev.get("event_type") or "progress_event",
                ts,
                _as_int(ev.get("duration_seconds")),
                _as_float(ev.get("score")),
                ev.get("module_id"),
                _as_float(ev.get("completion_percentage")),
                bool(ev.get("is_valid", True)),
                ts is not None,
                ev.get("score") is not None,
                bool(ev.get("is_duplicate", False)),
                int(ev.get("quality_score", 100)),
                self.batch_id,
            ])
        if not rows:
            return 0
        placeholders = ",".join(cols)
        updates = ",".join([f"{c}=EXCLUDED.{c}" for c in cols if c != "event_id"])
        stmt = f"INSERT INTO {self.table} ({placeholders}) VALUES %s ON CONFLICT (event_id) DO UPDATE SET {updates}"
        with psycopg2.connect(self.conn_str) as conn:
            with conn.cursor() as cur:
                execute_values(cur, stmt, rows)
        return len(rows)

    def _parse_ts(self, raw: Optional[str]) -> Optional[datetime]:
        if not raw:
            return None
        try:
            return datetime.fromisoformat(raw)
        except Exception:
            try:
                return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
            except Exception:
                return None


def _as_int(val) -> Optional[int]:
    try:
        return int(val)
    except Exception:
        return None


def _as_float(val) -> Optional[float]:
    try:
        return float(val)
    except Exception:
        return None
