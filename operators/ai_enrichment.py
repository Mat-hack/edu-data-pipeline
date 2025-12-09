"""AIEnrichmentOperator with optional LangGraph/LLM call and metrics."""
import json
import os
from typing import Callable, Dict, Iterable, List, Optional

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

try:
    from prometheus_client import Counter, Histogram, REGISTRY, start_http_server
except Exception:  # pragma: no cover
    Counter = None
    Histogram = None
    REGISTRY = None
    start_http_server = None

try:
    import requests
except Exception:  # pragma: no cover
    requests = None


GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"


def _collector(ctor, name: str, desc: str):
    """Return existing collector if already registered to avoid duplicate errors."""
    if not ctor or not REGISTRY:
        return None
    existing = getattr(REGISTRY, "_names_to_collectors", {}).get(name)
    if existing:
        return existing
    try:
        return ctor(name, desc)
    except ValueError:
        return getattr(REGISTRY, "_names_to_collectors", {}).get(name)


LLM_CALLS = _collector(Counter, "llm_calls_total", "Number of LLM enrichment calls")
LLM_LATENCY = _collector(Histogram, "llm_latency_seconds", "LLM enrichment latency")
LLM_TOKENS = _collector(Counter, "llm_tokens_total", "Approx tokens sent to LLM")

if start_http_server:
    try:
        port = int(os.environ.get("METRICS_PORT", "8001"))
        start_http_server(port)
    except Exception:
        pass


class AIEnrichmentOperator(BaseOperator):
    """Sends batches to an AI service and returns enriched records."""

    @apply_defaults
    def __init__(self, enrich_fn: Callable[[List[Dict]], List[Dict]], batch: Iterable[Dict] = None, xcom_task_id: Optional[str] = None, xcom_key: Optional[str] = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.enrich_fn = enrich_fn
        self.batch = list(batch or [])
        self.xcom_task_id = xcom_task_id
        self.xcom_key = xcom_key

    def execute(self, context):
        payload = self.batch
        if not payload and self.xcom_task_id and context:
            payload = context.get("ti").xcom_pull(task_ids=self.xcom_task_id, key=self.xcom_key) or []
        if not payload:
            self.log.info("No records to enrich")
            return []
        endpoint = os.environ.get("LANGGRAPH_ENDPOINT")
        try:
            groq_key = os.environ.get("GROQ_API_KEY")
            approx_tokens = int(len(json.dumps(payload)) / 4)
            if groq_key and requests:
                enriched = self._call_groq(payload, groq_key, approx_tokens)
            elif endpoint and requests:
                enriched = self._call_endpoint(payload, endpoint, approx_tokens)
            else:
                enriched = self.enrich_fn(payload)
            self.log.info("Enriched %s records", len(enriched))
            return enriched
        except Exception as exc:  # noqa: B902
            self.log.error("AI enrichment failed, using fallback stub: %s", exc)
            return self.enrich_fn(payload)

    def _call_groq(self, payload: List[Dict], api_key: str, approx_tokens: int) -> List[Dict]:
        if LLM_CALLS:
            LLM_CALLS.inc()
        timer = LLM_LATENCY.time() if LLM_LATENCY else None
        if LLM_TOKENS and approx_tokens > 0:
            LLM_TOKENS.inc(approx_tokens)
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        model = os.environ.get("GROQ_MODEL", "mixtral-8x7b-32768")
        system_prompt = "You are a data enrichment service. Return JSON echoing the input list with optional insight fields."
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(payload)},
        ]
        resp = requests.post(
            GROQ_API_URL,
            headers=headers,
            json={"model": model, "messages": messages, "response_format": {"type": "json_object"}},
            timeout=30,
        )
        if timer:
            timer.observe_duration()
        resp.raise_for_status()
        data = resp.json()
        content = data.get("choices", [{}])[0].get("message", {}).get("content")
        try:
            parsed = json.loads(content) if content else None
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, list):
            return parsed
        # If the provider returns an object, wrap to maintain list contract.
        return [parsed] if parsed else self.enrich_fn(payload)

    def _call_endpoint(self, payload: List[Dict], endpoint: str, approx_tokens: int) -> List[Dict]:
        if LLM_CALLS:
            LLM_CALLS.inc()
        timer = LLM_LATENCY.time() if LLM_LATENCY else None
        if LLM_TOKENS and approx_tokens > 0:
            LLM_TOKENS.inc(approx_tokens)
        resp = requests.post(endpoint, json=payload, timeout=30)
        if timer:
            timer.observe_duration()
        resp.raise_for_status()
        return resp.json()
