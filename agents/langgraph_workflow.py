"""LangGraph workflow stubs for AI agents."""
from typing import Dict, List


def route_event(payload: Dict) -> str:
    if payload.get("type") == "ticket":
        return "support_analyzer"
    if payload.get("type") == "quality":
        return "quality_analyzer"
    return "student_risk"


def student_risk_scorer(student: Dict) -> Dict:
    risk = 0
    if not student.get("recent_activity", True):
        risk += 20
    if (student.get("completion_rate") or 0) < 30:
        risk += 25
    if (student.get("payment_status") or "").lower() != "completed":
        risk += 15
    return {
        "risk_score": min(risk, 100),
        "risk_category": _bucket(risk),
        "factors": ["activity", "completion", "payment"],
    }


def support_ticket_analyzer(ticket: Dict) -> Dict:
    text = (ticket.get("subject", "") + " " + ticket.get("description", "")).lower()
    sentiment = "Negative" if "not" in text else "Neutral"
    score = -0.2 if sentiment == "Negative" else 0.0
    return {"sentiment": sentiment, "sentiment_score": score, "category": "general"}


def quality_analyzer(metrics: Dict, flagged_records: List[Dict]) -> Dict:
    top_issue = metrics.get("worst_field", "email")
    return {"summary": f"Top issue: {top_issue}", "recommendation": "Improve validation at source"}


def insight_generator(inputs: Dict) -> Dict:
    return {
        "executive_summary": ["Data processed", "Quality stable"],
        "actions": ["Remediate invalid emails", "Follow up with pending payments"],
    }


def _bucket(score: int) -> str:
    if score >= 75:
        return "Critical"
    if score >= 50:
        return "High"
    if score >= 25:
        return "Medium"
    return "Low"
