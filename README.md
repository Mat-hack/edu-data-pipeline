# EduFlow AI — Student Analytics Platform

EduFlow AI is a student analytics data platform designed to ingest, clean, transform, and model educational data into a structured analytics warehouse. The system supports batch ingestion with optional streaming and AI-based enrichment, while prioritizing data quality, reliability, and observability.

The pipeline follows a layered architecture and is orchestrated using **Apache Airflow**.

---

## Project Overview

### Objective

Build a reliable student analytics warehouse from CSV-based batch data, with optional streaming simulation and AI enrichment.

### Pipeline Flow

* Batch ingestion into raw tables
* Deterministic data cleaning and validation
* Dimensional modeling into an analytics warehouse
* Optional Kafka-based streaming simulation
* Optional AI-based enrichment with safe fallbacks

---

## Technology Stack

* **Apache Airflow** – workflow orchestration and scheduling
* **PostgreSQL** – transactional storage with constraints and upserts
* **Docker Compose** – reproducible local infrastructure
* **Kafka (optional)** – event-based streaming simulation
* **Prometheus** – metrics and observability

---

## Quick Start

```bash
cp .env.example .env
# GROQ_API_KEY is optional

docker-compose build airflow kafka
docker-compose up -d

# Trigger pipelines
docker-compose exec airflow airflow trigger_dag file_ingestion_pipeline
docker-compose exec airflow airflow trigger_dag data_cleaning_pipeline
docker-compose exec airflow airflow trigger_dag data_transformation_pipeline
```

---

## Service URLs

* **Airflow UI:** [http://localhost:8080](http://localhost:8080)
  (airflow / airflow)

* **Prometheus:** [http://localhost:9090](http://localhost:9090)

* **PostgreSQL:** localhost:5432
  (eduflow / eduflowpass)

---

## Data Architecture

### Raw Layer (`raw.*`)

* Append-only tables
* Mirrors source CSV files
* Used for replay and debugging

### Staging Layer (`staging.*`)

* Cleaned and standardized records
* Idempotent upserts based on natural keys
* Quality flags and duplicate detection

### Warehouse Layer (`warehouse.*`)

* Star schema design
* Dimension and fact tables
* Type-1 dimension updates
* Facts generated only from validated staging data

---

## Data Cleaning & Validation

Data cleaning logic is implemented in `dags/utils/cleaning_rules.py` and includes:

* Student ID normalization
* Name and email standardization
* Phone number normalization
* Date parsing with rejection handling
* Gender and location normalization
* Numeric bounds validation
* Duplicate detection

Records failing hard validation rules are excluded.
Soft issues are retained with quality flags for downstream analysis.

---

## Airflow Pipelines

### `file_ingestion_pipeline`

* Validates input files
* Loads CSV data into raw tables

### `data_cleaning_pipeline`

* Applies cleaning and validation rules
* Upserts records into staging tables
* Logs data quality metadata

### `data_transformation_pipeline`

* Derives analytical fields
* Aggregates student progress
* Loads dimension and fact tables
* Optionally applies AI enrichment

---

## Streaming Simulation (Optional)

Located in `kafka/streaming.py`:

* Kafka producer emits CSV rows as events
* Real-time cleaner applies the same validation rules as batch
* Aggregator computes rolling metrics

This module is decoupled from the batch ETL and can be enabled independently.

---

## AI Enrichment (Optional)

* Default stub implementation (deterministic)
* Optional LLM-based enrichment when `GROQ_API_KEY` is provided
* Automatic fallback on errors or timeouts
* Metrics emitted for AI calls and latency

AI output is not required for pipeline correctness.

---

## Observability

Prometheus collects metrics from:

* Airflow
* Kafka services

Tracked metrics include:

* Records processed per pipeline
* Pipeline execution time
* AI call counts and failures

---

## Sample Run Summary

| Layer     | Table                 | Rows |
| --------- | --------------------- | ---: |
| raw       | students_enrollment   |   93 |
| raw       | student_progress      |  153 |
| raw       | course_catalog        |   30 |
| raw       | support_tickets       |   45 |
| staging   | stg_students          |   30 |
| staging   | stg_progress          |   50 |
| staging   | stg_tickets           |   15 |
| warehouse | dim_students          |   30 |
| warehouse | dim_courses           |   10 |
| warehouse | fact_student_progress |   50 |
| warehouse | fact_support_tickets  |   15 |
| warehouse | fact_enrollments      |    1 |

---

## Testing

```bash
python -m pip install -r requirements-dev.txt
PYTHONPATH=. pytest -q tests
```

Tests focus on cleaning logic and idempotent database operations.

---

## Project Structure

```
dags/        # Airflow DAGs and orchestration
operators/   # Custom Airflow operators
kafka/       # Streaming simulation
agents/      # AI enrichment stubs
init_db/     # Database schema and constraints
output/      # Run artifacts
```

---

## Future Improvements

* Replace CSV ingestion with CDC-based ingestion
* Implement SCD Type-2 dimensions
* Add Grafana dashboards and alerting
* Automate backfills and replay workflows

---
