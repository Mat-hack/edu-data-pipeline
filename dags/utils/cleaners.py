"""Composable cleaning functions for different datasets."""
from datetime import datetime
from typing import Dict, Tuple

from . import cleaning_rules as rules


def clean_student_record(record: Dict, city_master_path: str = None) -> Tuple[Dict, Dict]:
    student_id_res = rules.standardize_student_id(record.get("student_id"))
    name_res = rules.clean_name(record.get("full_name"))
    email_res = rules.validate_email(record.get("email"))
    phone_res = rules.standardize_phone(record.get("phone"))
    dob_res = rules.parse_date(record.get("dob"))
    gender_res = rules.standardize_gender(record.get("gender"))
    city_res = rules.clean_city(record.get("city"), city_master_path=city_master_path)
    state_res = rules.clean_state(record.get("state"))
    enroll_date_res = rules.parse_date(record.get("enrollment_date"))
    fee_res = rules.clean_numeric(record.get("fee_paid"))
    payment_status_raw = (record.get("payment_status") or "").strip()
    cleaned_payment_status = payment_status_raw.title() if payment_status_raw else "Unknown"

    cleaning_map = {
        "student_id": student_id_res,
        "name": name_res,
        "email": email_res,
        "phone": phone_res,
        "dob": dob_res,
        "gender": gender_res,
        "city": city_res,
        "state": state_res,
        "enrollment_date": enroll_date_res,
        "fee_paid": fee_res,
    }
    flags = {k: v.is_valid for k, v in cleaning_map.items()}
    cleaned = {
        "student_id": student_id_res.value,
        "first_name": name_res.value.get("first_name") if name_res.is_valid else None,
        "last_name": name_res.value.get("last_name") if name_res.is_valid else None,
        "full_name": name_res.value.get("full_name") if name_res.is_valid else None,
        "email": email_res.value,
        "phone": phone_res.value,
        "dob": dob_res.value,
        "gender": gender_res.value,
        "city": city_res.value,
        "state": state_res.value,
        "enrollment_date": enroll_date_res.value,
        "program_id": (record.get("program_id") or "").upper() or None,
        "fee_paid": fee_res.value,
        "payment_status": cleaned_payment_status,
    }
    summary = rules.cleaning_summary(cleaning_map)
    audit = {"record_id": record.get("student_id"), "flags": flags, "quality_score": summary["quality_score"]}
    return cleaned, audit


def clean_progress_record(record: Dict) -> Tuple[Dict, Dict]:
    event_id = record.get("event_id")
    student_id_res = rules.standardize_student_id(record.get("student_id"))
    score_res = rules.validate_score(record.get("score"))
    completion_res = rules.validate_score(record.get("completion_percentage"))
    duration_res = rules.clean_numeric(record.get("duration_seconds"))
    ts_res = rules.parse_date_time(record.get("event_timestamp")) if hasattr(rules, "parse_date_time") else None
    cleaned = {
        "event_id": event_id,
        "student_id": student_id_res.value,
        "course_id": record.get("course_id"),
        "event_type": record.get("event_type"),
        "event_timestamp": ts_res.value if ts_res else None,
        "duration_seconds": int(duration_res.value) if duration_res.value is not None else None,
        "score": score_res.value,
        "module_id": record.get("module_id"),
        "completion_percentage": completion_res.value,
        "is_student_valid": student_id_res.is_valid,
        "is_timestamp_valid": ts_res.is_valid if ts_res else False,
        "is_score_valid": score_res.is_valid,
        "is_duplicate": False,
    }
    flags = {
        "student": student_id_res.is_valid,
        "score": score_res.is_valid,
        "completion": completion_res.is_valid,
        "duration": duration_res.is_valid,
        "timestamp": ts_res.is_valid if ts_res else False,
    }
    quality = rules.quality_score(flags)
    audit = {"record_id": event_id, "flags": flags, "quality_score": quality}
    cleaned["quality_score"] = quality
    cleaned["flags"] = flags
    return cleaned, audit


def clean_ticket_record(record: Dict) -> Tuple[Dict, Dict]:
    student_id_res = rules.standardize_student_id(record.get("student_id"))
    created_res = rules.parse_date(record.get("created_date"))
    resolved_res = rules.parse_date(record.get("resolved_date"))
    cleaned = {
        "ticket_id": record.get("ticket_id"),
        "student_id": student_id_res.value,
        "subject": record.get("subject"),
        "description": record.get("description"),
        "priority": record.get("priority"),
        "status": record.get("status"),
        "created_date": created_res.value,
        "resolved_date": resolved_res.value,
        "is_student_valid": student_id_res.is_valid,
    }
    flags = {"student": student_id_res.is_valid, "created": created_res.is_valid, "resolved": resolved_res.is_valid}
    quality = rules.quality_score(flags)
    audit = {"record_id": record.get("ticket_id"), "flags": flags, "quality_score": quality}
    cleaned["quality_score"] = quality
    cleaned["flags"] = flags
    return cleaned, audit
