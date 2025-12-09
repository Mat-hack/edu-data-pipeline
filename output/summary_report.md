# Pipeline Run Summary

## DAG status
- file_ingestion_pipeline: SUCCESS (latest manual runs)
- data_cleaning_pipeline: SUCCESS
- data_transformation_pipeline: SUCCESS

## Table row counts
| table | count |
| --- | ---: |
| raw.students_enrollment | 93 |
| raw.student_progress | 153 |
| raw.course_catalog | 30 |
| raw.support_tickets | 45 |
| staging.stg_students | 30 |
| staging.stg_progress | 50 |
| staging.stg_tickets | 15 |
| staging.stg_quality_log | 0 |
| warehouse.dim_date | 2557 |
| warehouse.dim_students | 30 |
| warehouse.dim_courses | 10 |
| warehouse.fact_student_progress | 50 |
| warehouse.fact_enrollments | 1 |
| warehouse.fact_support_tickets | 15 |

## Tests
- Command: `PYTHONPATH=. pytest -q tests`
- Result: **8 passed**

## Notes
- AI enrichment now supports Groq via `GROQ_API_KEY` with metrics; falls back to stub when unset.
- Airflow consumes environment variables from `.env` (see `.env.example`).
- `staging.stg_quality_log` is 0 in this run because no records were flagged as invalid.

## Kafka streaming sink smoke test
- Command: `docker-compose exec -T kafka python - <<'PY'` … inserts into `staging.stg_progress` using `PG_DSN`
- Result: `inserted smoke-20251208213532834926` then `psql` showed the row present with `quality_score=100`
- Purpose: validates container `PG_DSN` wiring and staging schema (`event_timestamp` column) end-to-end
