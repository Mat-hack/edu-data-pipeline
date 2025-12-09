"""Airflow DAG: file_ingestion_pipeline."""
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from operators.file_validation import FileValidationOperator
from dags.utils import db

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

INPUT_DIR = Path(os.environ.get("INPUT_DIR", "/usr/local/airflow/input_data"))
ASSIGNMENT_DIR = Path(__file__).resolve().parents[1] / "University Hiring Assignment -_ Data Engineer"


def _resolve_file(filename: str) -> Path:
    candidate = INPUT_DIR / filename
    if candidate.exists():
        return candidate
    fallback = ASSIGNMENT_DIR / filename
    # Fall back to the bundled assignment data if the mounted input path is empty.
    return fallback


def detect_new_files(**context):
    files = ["students_enrollment.csv", "student_progress.csv", "course_catalog.csv", "support_tickets.csv"]
    found = []
    for f in files:
        path = _resolve_file(f)
        if path.exists():
            found.append(str(path))
    context["ti"].xcom_push(key="ingestion_files", value=found)
    return found


def ingest_csv(path: str) -> List[Dict]:
    try:
        import pandas as pd

        df = pd.read_csv(path)
        return df.to_dict(orient="records")
    except Exception:
        import csv

        with open(path, "r", newline="") as f:
            reader = csv.DictReader(f)
            return list(reader)


def ingest_file(file_path: str, **context):
    records = ingest_csv(file_path)
    context["ti"].xcom_push(key=Path(file_path).name, value=records)
    return len(records)


def load_to_staging(**context):
    files = context["ti"].xcom_pull(key="ingestion_files") or []
    counts = {}
    table_map = {
        "students_enrollment.csv": ("raw.students_enrollment", [
            "student_id","full_name","email","phone","dob","gender","city","state","enrollment_date","program_id","fee_paid","payment_status","file_name","file_row_number","batch_id"
        ]),
        "student_progress.csv": ("raw.student_progress", [
            "event_id","student_id","course_id","event_type","event_timestamp","duration_seconds","score","module_id","completion_percentage","file_name","file_row_number","batch_id"
        ]),
        "course_catalog.csv": ("raw.course_catalog", [
            "course_id","course_name","category","difficulty","duration_hours","price","instructor_name","is_active"
        ]),
        "support_tickets.csv": ("raw.support_tickets", [
            "ticket_id","student_id","subject","description","priority","status","category","created_date","resolved_date","pdf_page_number","batch_id"
        ]),
    }
    for f in files:
        key = Path(f).name
        records = context["ti"].xcom_pull(key=key) or []
        table, cols = table_map.get(key, (None, None))
        if table:
            for i, rec in enumerate(records, start=1):
                rec.setdefault("file_name", key)
                rec.setdefault("file_row_number", i)
                rec.setdefault("batch_id", context.get("dag_run").run_id if context.get("dag_run") else "manual")
            db.insert_rows(table, records, cols)
        counts[key] = len(records)
    return counts


def update_ingestion_log(**context):
    counts = context["ti"].xcom_pull(task_ids="load_to_staging") or {}
    return {"ingested": counts, "logged_at": datetime.utcnow().isoformat()}


with DAG(
    dag_id="file_ingestion_pipeline",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
) as dag:
    detect_new = PythonOperator(task_id="detect_new_files", python_callable=detect_new_files, provide_context=True)

    validate_students = FileValidationOperator(
        task_id="validate_students",
        file_path=str(_resolve_file("students_enrollment.csv")),
        required_columns=[
            "student_id",
            "full_name",
            "email",
            "phone",
            "dob",
            "gender",
            "city",
            "state",
            "enrollment_date",
            "program_id",
            "fee_paid",
            "payment_status",
        ],
    )

    validate_progress = FileValidationOperator(
        task_id="validate_progress",
        file_path=str(_resolve_file("student_progress.csv")),
        required_columns=[
            "event_id",
            "student_id",
            "course_id",
            "event_type",
            "event_timestamp",
            "duration_seconds",
            "score",
            "module_id",
            "completion_percentage",
        ],
    )

    validate_courses = FileValidationOperator(
        task_id="validate_courses",
        file_path=str(_resolve_file("course_catalog.csv")),
        required_columns=[
            "course_id",
            "course_name",
            "category",
            "difficulty",
            "duration_hours",
            "price",
            "instructor_name",
        ],
    )

    validate_tickets = FileValidationOperator(
        task_id="validate_tickets",
        file_path=str(_resolve_file("support_tickets.csv")),
        required_columns=[
            "ticket_id",
            "student_id",
            "subject",
            "description",
            "priority",
            "status",
            "created_date",
            "resolved_date",
        ],
    )

    ingest_students = PythonOperator(
        task_id="ingest_excel_students",
        python_callable=ingest_file,
        op_kwargs={"file_path": str(_resolve_file("students_enrollment.csv"))},
        provide_context=True,
    )

    ingest_progress = PythonOperator(
        task_id="ingest_csv_progress",
        python_callable=ingest_file,
        op_kwargs={"file_path": str(_resolve_file("student_progress.csv"))},
        provide_context=True,
    )

    ingest_courses = PythonOperator(
        task_id="ingest_csv_courses",
        python_callable=ingest_file,
        op_kwargs={"file_path": str(_resolve_file("course_catalog.csv"))},
        provide_context=True,
    )

    ingest_tickets = PythonOperator(
        task_id="ingest_tickets",
        python_callable=ingest_file,
        op_kwargs={"file_path": str(_resolve_file("support_tickets.csv"))},
        provide_context=True,
    )

    load_staging = PythonOperator(task_id="load_to_staging", python_callable=load_to_staging, provide_context=True)

    ingestion_log = PythonOperator(task_id="update_ingestion_log", python_callable=update_ingestion_log, provide_context=True)

    validations = [validate_students, validate_progress, validate_courses, validate_tickets]
    ingestions = [ingest_students, ingest_progress, ingest_courses, ingest_tickets]

    # Ensure new files trigger validations, then corresponding ingestions, before staging/logging
    detect_new >> validations
    for validate_task, ingest_task in zip(validations, ingestions):
        validate_task >> ingest_task
    ingestions >> load_staging >> ingestion_log
