# Architecture & Design Notes

## Overview
The solution follows a layered model: raw -> staging -> warehouse -> analytics. Airflow orchestrates batch pipelines, Kafka simulates streaming progress events, and AI enrichment is stubbed with LangGraph-style agents.

## Key choices
- **Airflow (LocalExecutor)**: Built from `Dockerfile.airflow` extending puckel image with pandas, psycopg2-binary, prometheus_client, requests. Mounts DAGs/operators directly.
- **Postgres warehouse**: Single database with schemas for raw/staging/warehouse/analytics/metadata. See `init_db/init_schema.sql`.
- **Python utilities**: Cleaning and transformations implemented as pure Python (no heavy deps) for portability inside Airflow containers.
- **Kafka simulation**: Producer/consumer classes in `kafka/` to demonstrate topology without requiring external services beyond docker-compose. Kafka image extended via `Dockerfile.kafka` to include Python + prometheus_client for metrics when needed.
- **AI stubs**: `agents/langgraph_workflow.py` mirrors the required agents so they can be swapped with real LangGraph nodes.

## Data flow (batch)
1. **file_ingestion_pipeline**: detect files, validate headers/size, ingest CSVs, load into `raw.*` tables.
2. **data_cleaning_pipeline**: apply cleaning rules (IDs, names, email, phone, dates, gender, city/state, numerics), detect duplicates, compute quality flags, upsert into `staging.stg_students`, `staging.stg_progress`, and `staging.stg_tickets`.
3. **data_transformation_pipeline**: derive age/enrollment fields, map payment/enrollment status, aggregate progress/course metrics, AI enrich, upsert into `warehouse.dim_students`, load `warehouse.dim_courses`, and populate `warehouse.fact_student_progress`, `warehouse.fact_enrollments`, and `warehouse.fact_support_tickets`.

## Cleaning rules mapping
Implemented in `dags/utils/cleaning_rules.py` and `dags/utils/cleaners.py` to match the assignment’s 10 rules. Flags and a `quality_score` (100 - 10 per invalid field) are returned for downstream use.

## Streaming flow
`EventSimulatorProducer` reads CSV rows and emits to Kafka topics at a configurable rate. `RealTimeProcessor` cleans and checks anomalies, writing to processed and alert topics; `StreamAggregator` keeps rolling counts/error rates.

## AI/LangGraph flow
Router selects agent based on payload type; student risk scorer applies deterministic heuristics; support analyzer produces simple sentiment/category; quality analyzer summarizes flagged metrics; insight generator outputs a markdown-friendly summary. Replace stubs with real LangGraph graph and LLM prompts.

## Observability hooks
Counters/gauges described in the assignment can be exported by wrapping operator steps and Kafka processors with Prometheus or StatsD. Stubs log counts and flags for visibility; wiring metrics would be the next increment.

## Trade-offs & future work
- DB writes are placeholders to keep the sample runnable without extra deps; add psycopg2 + SQLAlchemy sessions for full ETL.
- Airflow tasks currently reuse local CSVs; move to S3/file-watcher triggers in production.
- Fuzzy city matching uses a lightweight similarity; swap with `rapidfuzz` for better accuracy.
- AI enrichment now attempts a `LANGGRAPH_ENDPOINT` call when set; otherwise mocked. Metrics are exposed via `prometheus_client` HTTP server on `METRICS_PORT`. Next step: integrate LangGraph/LLM clients with retries, batching, and token metrics.
- Add data validation tests and contract tests for each DAG task.
