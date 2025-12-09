"""Airflow DAG: data_cleaning_pipeline."""
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from operators.data_cleaning import DataCleaningOperator
from dags.utils.cleaners import clean_student_record, clean_progress_record, clean_ticket_record
from dags.utils import cleaning_rules, db

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

ASSIGNMENT_DIR = Path(__file__).resolve().parents[1] / "University Hiring Assignment -_ Data Engineer"
INPUT_DIR = Path(os.environ.get("INPUT_DIR", "/usr/local/airflow/input_data"))
# Prefer mounted input_data; fall back to bundled assignment copy if not mounted.
CITY_MASTER_PATH = (INPUT_DIR / "city_master.csv") if (INPUT_DIR / "city_master.csv").exists() else (ASSIGNMENT_DIR / "city_master.csv")


def summarize_quality(**context):
    audit = context["ti"].xcom_pull(task_ids="clean_student_ids") or {}
    audits = audit.get("audit", [])
    invalid_counts = {"invalid_records": 0}
    for a in audits:
        if a.get("quality_score", 100) < 100:
            invalid_counts["invalid_records"] += 1
    invalid_counts["total"] = len(audits)
    context["ti"].xcom_push(key="quality_summary", value=invalid_counts)
    return invalid_counts


def mark_duplicates(**context):
    cleaned = context["ti"].xcom_pull(task_ids="clean_student_ids") or {}
    records = cleaned.get("cleaned", [])
    ids = [r.get("student_id") for r in records]
    dup_flags = cleaning_rules.detect_exact_duplicates(ids)
    for r in records:
        r["is_duplicate"] = dup_flags.get(r.get("student_id"), False)
    context["ti"].xcom_push(key="cleaned_students", value=records)
    return {"duplicates": sum(1 for v in dup_flags.values() if v), "total": len(ids)}


def move_to_cleaned(**context):
    cleaned_records = context["ti"].xcom_pull(key="cleaned_students") or []
    # Deduplicate by student_id to avoid ON CONFLICT touching same row twice
    deduped = {}
    for rec in cleaned_records:
        sid = rec.get("student_id")
        if sid is None:
            continue
        deduped[sid] = rec
    cleaned_records = list(deduped.values())
    columns = [
        "student_id",
        "first_name",
        "last_name",
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
        "is_email_valid",
        "is_phone_valid",
        "is_date_valid",
        "is_duplicate",
        "quality_score",
        "cleaning_notes",
        "batch_id",
    ]
    for rec in cleaned_records:
        flags = rec.get("flags", {}) if isinstance(rec, dict) else {}
        rec.setdefault("is_email_valid", flags.get("email", True))
        rec.setdefault("is_phone_valid", flags.get("phone", True))
        rec.setdefault("is_date_valid", flags.get("dob", True))
        rec.setdefault("is_duplicate", rec.get("is_duplicate", False))
        rec.setdefault("quality_score", rec.get("quality_score", 100))
        rec.setdefault("cleaning_notes", str(flags))
        rec.setdefault("batch_id", context.get("dag_run").run_id if context.get("dag_run") else "manual")
    inserted = db.upsert_rows("staging.stg_students", cleaned_records, columns, ["student_id"])
    return {"moved": inserted}


with DAG(
    dag_id="data_cleaning_pipeline",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
) as dag:
    clean_students = DataCleaningOperator(
        task_id="clean_student_ids",
        cleaning_fn=lambda rec: clean_student_record(rec, city_master_path=str(CITY_MASTER_PATH)),
        file_path=str(INPUT_DIR / "students_enrollment.csv"),
    )

    clean_progress = DataCleaningOperator(
        task_id="clean_progress",
        cleaning_fn=clean_progress_record,
        file_path=str(INPUT_DIR / "student_progress.csv"),
    )

    clean_tickets = DataCleaningOperator(
        task_id="clean_tickets",
        cleaning_fn=clean_ticket_record,
        file_path=str(INPUT_DIR / "support_tickets.csv"),
    )

    clean_names = PythonOperator(task_id="clean_names", python_callable=lambda: True)
    validate_emails = PythonOperator(task_id="validate_emails", python_callable=lambda: True)
    standardize_phones = PythonOperator(task_id="standardize_phones", python_callable=lambda: True)
    parse_dates = PythonOperator(task_id="parse_dates", python_callable=lambda: True)
    standardize_gender = PythonOperator(task_id="standardize_gender", python_callable=lambda: True)
    clean_locations = PythonOperator(task_id="clean_locations", python_callable=lambda: True)
    clean_numerics = PythonOperator(task_id="clean_numerics", python_callable=lambda: True)

    detect_duplicates = PythonOperator(task_id="detect_duplicates", python_callable=mark_duplicates, provide_context=True)
    calculate_quality_scores = PythonOperator(task_id="calculate_quality_scores", python_callable=summarize_quality, provide_context=True)
    move_to_cleaned_task = PythonOperator(task_id="move_to_cleaned", python_callable=move_to_cleaned, provide_context=True)

    move_progress = PythonOperator(
        task_id="move_progress_to_cleaned",
        python_callable=lambda **ctx: db.upsert_rows(
            "staging.stg_progress",
            [
                {
                    **rec,
                    "batch_id": ctx.get("dag_run").run_id if ctx.get("dag_run") else "manual",
                    "quality_score": rec.get("quality_score", 100),
                }
                for rec in (ctx["ti"].xcom_pull(task_ids="clean_progress") or {}).get("cleaned", [])
            ],
            [
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
            ],
            ["event_id"],
        ),
        provide_context=True,
    )

    move_tickets = PythonOperator(
        task_id="move_tickets_to_cleaned",
        python_callable=lambda **ctx: db.upsert_rows(
            "staging.stg_tickets",
            [
                {
                    **rec,
                    "batch_id": ctx.get("dag_run").run_id if ctx.get("dag_run") else "manual",
                    "quality_score": rec.get("quality_score", 100),
                }
                for rec in (ctx["ti"].xcom_pull(task_ids="clean_tickets") or {}).get("cleaned", [])
            ],
            [
                "ticket_id",
                "student_id",
                "subject",
                "description",
                "priority",
                "status",
                "category",
                "created_date",
                "resolved_date",
                "ai_sentiment",
                "ai_sentiment_score",
                "ai_category_suggestion",
                "ai_priority_suggestion",
                "is_student_valid",
                "quality_score",
                "batch_id",
            ],
            ["ticket_id"],
        ),
        provide_context=True,
    )

    clean_students >> [clean_names, validate_emails, standardize_phones, parse_dates, standardize_gender, clean_locations, clean_numerics]
    [clean_names, validate_emails, standardize_phones, parse_dates, standardize_gender, clean_locations, clean_numerics] >> detect_duplicates
    detect_duplicates >> calculate_quality_scores >> move_to_cleaned_task

    clean_progress >> move_progress
    clean_tickets >> move_tickets
