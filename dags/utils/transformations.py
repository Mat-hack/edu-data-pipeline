"""Transformation helpers for EduFlow pipelines."""
from datetime import date, datetime
from math import ceil
from typing import Dict, Iterable, List, Optional

from .cleaning_rules import PAYMENT_STATUS_MAP


def derive_age(dob: Optional[date]) -> Optional[int]:
    if not dob:
        return None
    today = date.today()
    years = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
    return max(years, 0)


def age_group(age: Optional[int]) -> Optional[str]:
    if age is None:
        return None
    if age <= 22:
        return "18-22"
    if age <= 27:
        return "23-27"
    if age <= 35:
        return "28-35"
    return "35+"


def enrollment_fields(enrollment_date: Optional[date]) -> Dict[str, Optional[int]]:
    if enrollment_date is None:
        return {"enrollment_month": None, "enrollment_year": None, "enrollment_quarter": None}
    month = enrollment_date.month
    return {
        "enrollment_month": month,
        "enrollment_year": enrollment_date.year,
        "enrollment_quarter": ceil(month / 3),
    }


def map_payment_status(raw: Optional[str]) -> str:
    key = (raw or "").strip().lower()
    return PAYMENT_STATUS_MAP.get(key, "UNKNOWN")


def derive_enrollment_status(payment_status: str, has_recent_activity: bool, last_activity: Optional[datetime]) -> str:
    if payment_status == "COMPLETED" and has_recent_activity:
        return "ACTIVE"
    if last_activity is None:
        return "PENDING" if payment_status != "COMPLETED" else "INACTIVE"
    days_since = (datetime.utcnow() - last_activity).days
    if days_since >= 90:
        return "CHURNED"
    if days_since >= 30:
        return "INACTIVE"
    if payment_status != "COMPLETED":
        return "PENDING"
    return "ACTIVE"


def summarize_student_progress(events: Iterable[Dict]) -> Dict[str, object]:
    total_courses = set()
    modules_completed = 0
    scores: List[float] = []
    total_seconds = 0
    last_activity = None
    activity_7 = 0
    activity_30 = 0
    for event in events:
        course_id = event.get("course_id")
        if course_id:
            total_courses.add(course_id)
        if float(event.get("completion_percentage", 0) or 0) >= 100:
            modules_completed += 1
        score_val = event.get("score")
        if score_val is not None:
            try:
                scores.append(float(score_val))
            except (TypeError, ValueError):
                pass
        total_seconds += int(event.get("duration_seconds") or 0)
        ts = event.get("event_timestamp")
        if isinstance(ts, datetime):
            # Align timezone awareness to avoid naive/aware subtraction errors
            now = datetime.now(ts.tzinfo) if ts.tzinfo else datetime.utcnow()
            if last_activity is None or ts > last_activity:
                last_activity = ts
            if (now - ts).days < 7:
                activity_7 += 1
            if (now - ts).days < 30:
                activity_30 += 1
    avg_score = sum(scores) / len(scores) if scores else None
    return {
        "total_courses_enrolled": len(total_courses),
        "total_modules_completed": modules_completed,
        "avg_score": avg_score,
        "total_time_spent_hours": total_seconds / 3600 if total_seconds else 0,
        "last_activity_date": last_activity.date() if last_activity else None,
        "activity_count_7_days": activity_7,
        "activity_count_30_days": activity_30,
    }


def summarize_course(events: Iterable[Dict]) -> Dict[str, object]:
    students = set()
    completion_rates: List[float] = []
    scores: List[float] = []
    for event in events:
        student_id = event.get("student_id")
        if student_id:
            students.add(student_id)
        try:
            completion_rates.append(float(event.get("completion_percentage") or 0))
        except (TypeError, ValueError):
            pass
        try:
            if event.get("score") is not None:
                scores.append(float(event.get("score")))
        except (TypeError, ValueError):
            pass
    avg_completion = sum(completion_rates) / len(completion_rates) if completion_rates else None
    avg_score = sum(scores) / len(scores) if scores else None
    return {
        "total_enrollments": len(students),
        "avg_completion_rate": avg_completion,
        "avg_score": avg_score,
    }
