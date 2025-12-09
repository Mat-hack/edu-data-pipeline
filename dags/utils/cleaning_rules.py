"""Cleaning rule implementations for EduFlow data pipeline."""
import re
import csv
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Tuple


@dataclass
class RuleResult:
    """Represents the output of a cleaning rule."""
    value: Optional[object]
    is_valid: bool
    note: str = ""


DATE_FORMATS = [
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%d-%m-%Y",
    "%B %d, %Y",
    "%d-%b-%y",
]

DATETIME_FORMATS = [
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S%z",
]

PAYMENT_STATUS_MAP = {
    "paid": "COMPLETED",
    "pending": "PENDING",
    "partial": "PARTIAL",
    "refunded": "REFUNDED",
    "failed": "FAILED",
    "": "UNKNOWN",
}

DEFAULT_CITY_MASTER = [
    "Mumbai",
    "Bangalore",
    "Delhi",
    "Hyderabad",
    "Chennai",
    "Pune",
    "Kolkata",
    "Ahmedabad",
    "Jaipur",
    "Lucknow",
]


def _load_city_master(path: Optional[Path]) -> List[str]:
    if not path or not path.exists():
        return DEFAULT_CITY_MASTER
    with path.open("r", newline="") as f:
        reader = csv.reader(f)
        cities = [row[0].strip() for row in reader if row]
    return [c for c in cities if c]


def standardize_student_id(raw: str) -> RuleResult:
    if raw is None:
        return RuleResult(None, False, "missing")
    cleaned = re.sub(r"[^A-Za-z0-9]", "", str(raw)).upper()
    digits = re.sub(r"[^0-9]", "", cleaned)
    if not digits:
        return RuleResult(None, False, "no-digits")
    padded = digits.zfill(3)
    student_id = f"STU{padded}"
    return RuleResult(student_id, True, "standardized")


def clean_name(raw: str) -> RuleResult:
    if raw is None:
        return RuleResult(None, False, "missing")
    name = re.sub(r"\s+", " ", str(raw).strip())
    name = re.sub(r"[0-9]", "", name)
    name = name.title()
    if not name:
        return RuleResult(None, False, "empty")
    parts = name.split(" ")
    first_name = parts[0]
    last_name = " ".join(parts[1:]) if len(parts) > 1 else ""
    return RuleResult({"full_name": name, "first_name": first_name, "last_name": last_name}, True, "standardized")


def validate_email(raw: str) -> RuleResult:
    if raw is None:
        return RuleResult(None, False, "missing")
    email = str(raw).strip().lower()
    pattern = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    if re.match(pattern, email):
        return RuleResult(email, True, "valid")
    return RuleResult(None, False, "invalid-format")


def standardize_phone(raw: str) -> RuleResult:
    if raw is None:
        return RuleResult(None, False, "missing")
    phone = str(raw).strip()
    phone = phone.replace(" ", "")
    digits = re.sub(r"[^0-9]", "", phone)
    if len(digits) == 10:
        return RuleResult(f"+91-{digits}", True, "normalized-10")
    if len(digits) == 12 and digits.startswith("91"):
        return RuleResult(f"+91-{digits[2:]}", True, "normalized-12")
    return RuleResult(None, False, "invalid-length")


def parse_date(raw: str) -> RuleResult:
    if raw is None:
        return RuleResult(None, False, "missing")
    raw_str = str(raw).strip()
    for fmt in DATE_FORMATS:
        try:
            parsed = datetime.strptime(raw_str, fmt).date()
            if parsed.year < 1950:
                return RuleResult(None, False, "too-old")
            if parsed > datetime.now(timezone.utc).date():
                return RuleResult(None, False, "future-date")
            return RuleResult(parsed, True, fmt)
        except ValueError:
            continue
    return RuleResult(None, False, "unparsed")


def parse_date_time(raw: str) -> RuleResult:
    if raw is None:
        return RuleResult(None, False, "missing")
    raw_str = str(raw).strip()
    for fmt in DATETIME_FORMATS:
        try:
            parsed = datetime.strptime(raw_str, fmt)
            if parsed > datetime.now(timezone.utc):
                return RuleResult(None, False, "future-date")
            return RuleResult(parsed, True, fmt)
        except ValueError:
            continue
    return RuleResult(None, False, "unparsed")


def standardize_gender(raw: str) -> RuleResult:
    if raw is None:
        return RuleResult("Other", False, "missing")
    gender = str(raw).strip().lower()
    if gender in {"m", "male"}:
        return RuleResult("Male", True, "mapped")
    if gender in {"f", "female"}:
        return RuleResult("Female", True, "mapped")
    return RuleResult("Other", False, "defaulted")


def clean_city(raw: str, city_master_path: Optional[str] = None) -> RuleResult:
    if raw is None:
        return RuleResult(None, False, "missing")
    city = re.sub(r"\s+", " ", str(raw).strip()).title()
    corrections = {"Mumabi": "Mumbai", "Banglore": "Bangalore", "Pune": "Pune"}
    if city in corrections:
        city = corrections[city]
    city_master = _load_city_master(Path(city_master_path) if city_master_path else None)
    if city in city_master:
        return RuleResult(city, True, "matched")
    best_match = _closest(city, city_master)
    if best_match and best_match[1] >= 0.8:
        return RuleResult(best_match[0], True, f"fuzzy-{best_match[1]:.2f}")
    return RuleResult(city, False, "unmatched")


def clean_state(raw: str) -> RuleResult:
    if raw is None:
        return RuleResult(None, False, "missing")
    state = re.sub(r"\s+", " ", str(raw).strip()).title()
    return RuleResult(state, True, "normalized")


def clean_numeric(raw: str) -> RuleResult:
    if raw is None or str(raw).strip() == "":
        return RuleResult(0.0, False, "empty")
    val = re.sub(r"[^0-9.\-]", "", str(raw))
    try:
        number = float(val)
    except ValueError:
        return RuleResult(None, False, "not-a-number")
    flagged = False
    if number < 0:
        number = abs(number)
        flagged = True
    return RuleResult(number, not flagged, "abs" if flagged else "normalized")


def validate_score(raw: Optional[float]) -> RuleResult:
    if raw is None:
        return RuleResult(None, False, "missing")
    try:
        score = float(raw)
    except (TypeError, ValueError):
        return RuleResult(None, False, "invalid")
    flagged = False
    if score > 100:
        score = 100.0
        flagged = True
    if score < 0:
        score = 0.0
        flagged = True
    return RuleResult(round(score, 2), not flagged, "capped" if flagged else "valid")


def detect_exact_duplicates(values: Iterable[str]) -> Dict[str, bool]:
    seen = set()
    flags = {}
    for v in values:
        if v in seen:
            flags[v] = True
        else:
            seen.add(v)
            flags[v] = False
    return flags


def _closest(value: str, candidates: List[str]) -> Optional[Tuple[str, float]]:
    if not candidates:
        return None
    best = None
    for cand in candidates:
        ratio = _similarity(value, cand)
        if best is None or ratio > best[1]:
            best = (cand, ratio)
    return best


def _similarity(a: str, b: str) -> float:
    pairs = zip(a.lower(), b.lower())
    matches = sum(1 for x, y in pairs if x == y)
    return matches / max(len(a), len(b), 1)


def quality_score(flags: Dict[str, bool]) -> int:
    invalid_count = sum(1 for valid in flags.values() if not valid)
    return max(0, 100 - invalid_count * 10)


def cleaning_summary(rule_results: Dict[str, RuleResult]) -> Dict[str, object]:
    flags = {k: v.is_valid for k, v in rule_results.items()}
    return {"flags": flags, "quality_score": quality_score(flags)}
