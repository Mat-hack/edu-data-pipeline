"""Airflow DAG: data_transformation_pipeline."""
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from operators.ai_enrichment import AIEnrichmentOperator
from dags.utils import transformations, db

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

ASSIGNMENT_DIR = Path(__file__).resolve().parents[1] / "University Hiring Assignment -_ Data Engineer"


def _load_cleaned_students():
    return db.fetch_all("SELECT * FROM staging.stg_students")


def derive_age_fields(**context):
    students = _load_cleaned_students()
    for s in students:
        age_val = transformations.derive_age(s.get("dob"))
        s["age"] = age_val
        s["age_group"] = transformations.age_group(age_val)
        s.update(transformations.enrollment_fields(s.get("enrollment_date")))
    context["ti"].xcom_push(key="students_with_age", value=students)
    return students


def map_statuses(**context):
    students = context["ti"].xcom_pull(key="students_with_age") or []
    for s in students:
        mapped = transformations.map_payment_status(s.get("payment_status"))
        s["payment_status_std"] = mapped
        s["enrollment_status"] = transformations.derive_enrollment_status(
            mapped, has_recent_activity=True, last_activity=datetime.utcnow()
        )
    context["ti"].xcom_push(key="students_mapped", value=students)
    return students


def aggregate_student_progress(**context):
    events = _load_progress_events()
    summary = transformations.summarize_student_progress(events)
    context["ti"].xcom_push(key="student_progress_summary", value=summary)
    return summary


def _load_progress_events():
    return db.fetch_all("SELECT * FROM staging.stg_progress")


def aggregate_course_metrics(**context):
    events = _load_progress_events()
    summary = transformations.summarize_course(events)
    context["ti"].xcom_push(key="course_summary", value=summary)
    return summary


def create_quality_flags(**context):
    students = context["ti"].xcom_pull(key="students_mapped") or []
    for s in students:
        s["quality_score"] = 100
        s["is_email_valid"] = True
    context["ti"].xcom_push(key="students_ready", value=students)
    return {"students": len(students)}


def ai_enrich(records):
    enriched = []
    for r in records:
        enriched.append({**r, "ai_risk_score": 42.0, "ai_risk_category": "Medium"})
    return enriched


def load_to_warehouse(**context):
    enriched = context["ti"].xcom_pull(task_ids="ai_enrichment") or []
    student_cols = [
        "student_id",
        "first_name",
        "last_name",
        "full_name",
        "email",
        "phone",
        "dob",
        "age",
        "age_group",
        "gender",
        "city",
        "state",
        "enrollment_date",
        "enrollment_month",
        "enrollment_year",
        "enrollment_quarter",
        "program_id",
        "fee_paid",
        "payment_status",
        "ai_risk_score",
        "ai_risk_category",
        "quality_score",
    ]
    loaded = db.upsert_rows("warehouse.dim_students", enriched, student_cols, ["student_id"])
    return {"loaded": loaded}


def load_dim_courses(**context):
    courses = db.fetch_all("SELECT course_id, course_name, category, difficulty, duration_hours, price, instructor_name FROM raw.course_catalog")
    for c in courses:
        c["is_active"] = True
    cols = [
        "course_id",
        "course_name",
        "category",
        "difficulty",
        "duration_hours",
        "price",
        "instructor_name",
        "is_active",
    ]
    upserted = db.upsert_rows("warehouse.dim_courses", courses, cols, ["course_id"])
    return {"courses_upserted": upserted}


def _student_sk_map():
    rows = db.fetch_all("SELECT student_sk, student_id FROM warehouse.dim_students")
    return {r["student_id"]: r["student_sk"] for r in rows}


def _course_sk_map():
    rows = db.fetch_all("SELECT course_sk, course_id FROM warehouse.dim_courses")
    return {r["course_id"]: r["course_sk"] for r in rows}


def load_progress_facts(**context):
    events = db.fetch_all("SELECT * FROM staging.stg_progress")
    if not events:
        return {"loaded": 0}
    student_map = _student_sk_map()
    course_map = _course_sk_map()
    dates = [e.get("event_timestamp").date() for e in events if e.get("event_timestamp")]
    db.ensure_dim_dates(dates)
    fact_rows = []
    for e in events:
        ts = e.get("event_timestamp")
        date_key = ts.date().year * 10000 + ts.date().month * 100 + ts.date().day if ts else None
        fact_rows.append(
            {
                "student_sk": student_map.get(e.get("student_id")),
                "course_sk": course_map.get(e.get("course_id")),
                "date_key": date_key,
                "event_id": e.get("event_id"),
                "event_type": e.get("event_type"),
                "module_id": e.get("module_id"),
                "duration_seconds": e.get("duration_seconds"),
                "score": e.get("score"),
                "completion_percentage": e.get("completion_percentage"),
                "event_timestamp": ts,
            }
        )
    cols = [
        "student_sk",
        "course_sk",
        "date_key",
        "event_id",
        "event_type",
        "module_id",
        "duration_seconds",
        "score",
        "completion_percentage",
        "event_timestamp",
    ]
    loaded = db.upsert_rows("warehouse.fact_student_progress", fact_rows, cols, ["event_id"])
    return {"loaded": loaded}


def load_ticket_facts(**context):
    tickets = db.fetch_all("SELECT * FROM staging.stg_tickets")
    if not tickets:
        return {"loaded": 0}
    student_map = _student_sk_map()
    dates = []
    for t in tickets:
        if t.get("created_date"):
            dates.append(t.get("created_date"))
        if t.get("resolved_date"):
            dates.append(t.get("resolved_date"))
    db.ensure_dim_dates(dates)
    fact_rows = []
    for t in tickets:
        created = t.get("created_date")
        resolved = t.get("resolved_date")
        fact_rows.append(
            {
                "student_sk": student_map.get(t.get("student_id")),
                "created_date_key": created.year * 10000 + created.month * 100 + created.day if created else None,
                "resolved_date_key": resolved.year * 10000 + resolved.month * 100 + resolved.day if resolved else None,
                "ticket_id": t.get("ticket_id"),
                "category": t.get("category"),
                "priority": t.get("priority"),
                "resolution_time_hours": None,
                "status": t.get("status"),
                "ai_sentiment": t.get("ai_sentiment"),
                "ai_sentiment_score": t.get("ai_sentiment_score"),
            }
        )
    cols = [
        "student_sk",
        "created_date_key",
        "resolved_date_key",
        "ticket_id",
        "category",
        "priority",
        "resolution_time_hours",
        "status",
        "ai_sentiment",
        "ai_sentiment_score",
    ]
    loaded = db.upsert_rows("warehouse.fact_support_tickets", fact_rows, cols, ["ticket_id"])
    return {"loaded": loaded}


def generate_summary_report(**context):
    summary = {
        "students": context["ti"].xcom_pull(key="students_ready") or [],
        "student_progress_summary": context["ti"].xcom_pull(key="student_progress_summary") or {},
        "course_summary": context["ti"].xcom_pull(key="course_summary") or {},
    }
    return summary


def load_enrollments_fact(**context):
    students = db.fetch_all("SELECT * FROM staging.stg_students")
    if not students:
        return {"loaded": 0}
    student_map = _student_sk_map()
    dates = [s.get("enrollment_date") for s in students if s.get("enrollment_date")]
    db.ensure_dim_dates(dates)
    fact_rows = []
    for s in students:
        d = s.get("enrollment_date")
        date_key = d.year * 10000 + d.month * 100 + d.day if d else None
        fact_rows.append(
            {
                "student_sk": student_map.get(s.get("student_id")),
                "course_sk": None,
                "enrollment_date_key": date_key,
                "fee_paid": s.get("fee_paid"),
                "payment_status": s.get("payment_status"),
                "enrollment_status": s.get("enrollment_status", "PENDING"),
                "total_time_spent_minutes": 0,
                "modules_completed": 0,
                "avg_score": None,
                "completion_percentage": 0,
                "last_activity_date": None,
            }
        )
    cols = [
        "student_sk",
        "course_sk",
        "enrollment_date_key",
        "fee_paid",
        "payment_status",
        "enrollment_status",
        "total_time_spent_minutes",
        "modules_completed",
        "avg_score",
        "completion_percentage",
        "last_activity_date",
    ]
    loaded = db.upsert_rows("warehouse.fact_enrollments", fact_rows, cols, ["student_sk", "course_sk"])
    return {"loaded": loaded}


with DAG(
    dag_id="data_transformation_pipeline",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
) as dag:
    derive_age = PythonOperator(task_id="derive_age_fields", python_callable=derive_age_fields, provide_context=True)
    map_status = PythonOperator(task_id="map_statuses", python_callable=map_statuses, provide_context=True)
    aggregate_progress = PythonOperator(
        task_id="aggregate_student_progress", python_callable=aggregate_student_progress, provide_context=True
    )
    aggregate_course = PythonOperator(
        task_id="aggregate_course_metrics", python_callable=aggregate_course_metrics, provide_context=True
    )
    create_quality = PythonOperator(task_id="create_quality_flags", python_callable=create_quality_flags, provide_context=True)

    ai_enrichment = AIEnrichmentOperator(
        task_id="ai_enrichment",
        enrich_fn=ai_enrich,
        batch=[],
        xcom_task_id="create_quality_flags",
        xcom_key="students_ready",
    )

    load_warehouse = PythonOperator(task_id="load_to_warehouse", python_callable=load_to_warehouse, provide_context=True)
    load_courses = PythonOperator(task_id="load_dim_courses", python_callable=load_dim_courses, provide_context=True)
    load_progress = PythonOperator(task_id="load_progress_facts", python_callable=load_progress_facts, provide_context=True)
    load_tickets = PythonOperator(task_id="load_ticket_facts", python_callable=load_ticket_facts, provide_context=True)
    load_enrollments = PythonOperator(task_id="load_enrollments_fact", python_callable=load_enrollments_fact, provide_context=True)
    summary_report = PythonOperator(
        task_id="generate_summary_report", python_callable=generate_summary_report, provide_context=True
    )

    derive_age >> map_status >> create_quality >> ai_enrichment >> load_warehouse
    load_warehouse >> load_courses >> load_progress >> summary_report
    load_warehouse >> load_tickets >> summary_report
    load_warehouse >> load_enrollments >> summary_report
    derive_age >> aggregate_progress
    derive_age >> aggregate_course
