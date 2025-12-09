-- ============================================
-- EduFlow AI - Data Warehouse Schema
-- ============================================
-- For Data Engineer Assignment
-- ============================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================
-- SCHEMA CREATION
-- ============================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS metadata;

-- ============================================
-- RAW LAYER
-- Exact copy of source files, no transformations
-- ============================================

-- Raw: Students Enrollment (from Excel)
CREATE TABLE raw.students_enrollment (
    id SERIAL PRIMARY KEY,
    student_id VARCHAR(100),
    full_name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(100),
    dob VARCHAR(100),
    gender VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(100),
    enrollment_date VARCHAR(100),
    program_id VARCHAR(100),
    fee_paid VARCHAR(100),
    payment_status VARCHAR(100),
    
    -- Metadata
    file_name VARCHAR(255),
    file_row_number INTEGER,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    batch_id VARCHAR(100)
);

-- Raw: Student Progress (from CSV)
CREATE TABLE raw.student_progress (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(100),
    student_id VARCHAR(100),
    course_id VARCHAR(100),
    event_type VARCHAR(100),
    event_timestamp VARCHAR(100),
    duration_seconds VARCHAR(50),
    score VARCHAR(50),
    module_id VARCHAR(100),
    completion_percentage VARCHAR(50),
    
    -- Metadata
    file_name VARCHAR(255),
    file_row_number INTEGER,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    batch_id VARCHAR(100)
);

-- Raw: Course Catalog (from CSV)
CREATE TABLE raw.course_catalog (
    id SERIAL PRIMARY KEY,
    course_id VARCHAR(100),
    course_name VARCHAR(255),
    category VARCHAR(100),
    difficulty VARCHAR(50),
    duration_hours VARCHAR(50),
    price VARCHAR(50),
    instructor_name VARCHAR(255),
    is_active VARCHAR(10),
    
    -- Metadata
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Raw: Support Tickets (from PDF)
CREATE TABLE raw.support_tickets (
    id SERIAL PRIMARY KEY,
    ticket_id VARCHAR(100),
    student_id VARCHAR(100),
    subject VARCHAR(500),
    description TEXT,
    priority VARCHAR(50),
    status VARCHAR(50),
    category VARCHAR(100),
    created_date VARCHAR(100),
    resolved_date VARCHAR(100),
    
    -- Metadata
    pdf_page_number INTEGER,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    batch_id VARCHAR(100)
);

-- ============================================
-- STAGING LAYER
-- After cleaning, with quality flags
-- ============================================

-- Staging: Cleaned Students
CREATE TABLE staging.stg_students (
    id SERIAL PRIMARY KEY,
    raw_id INTEGER REFERENCES raw.students_enrollment(id),
    
    -- Cleaned fields
    student_id VARCHAR(20) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(20),
    dob DATE,
    gender VARCHAR(10),
    city VARCHAR(100),
    state VARCHAR(100),
    enrollment_date DATE,
    program_id VARCHAR(20),
    fee_paid DECIMAL(15, 2),
    payment_status VARCHAR(20),
    
    -- Quality flags
    is_email_valid BOOLEAN DEFAULT FALSE,
    is_phone_valid BOOLEAN DEFAULT FALSE,
    is_date_valid BOOLEAN DEFAULT FALSE,
    is_duplicate BOOLEAN DEFAULT FALSE,
    quality_score INTEGER DEFAULT 100,
    cleaning_notes TEXT,
    
    -- Processing metadata
    cleaned_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    batch_id VARCHAR(100),
    
    CONSTRAINT uq_stg_student_id UNIQUE (student_id)
);

CREATE INDEX idx_stg_students_quality ON staging.stg_students(quality_score);
CREATE INDEX idx_stg_students_email ON staging.stg_students(email);

-- Staging: Cleaned Progress Events
CREATE TABLE staging.stg_progress (
    id SERIAL PRIMARY KEY,
    raw_id INTEGER REFERENCES raw.student_progress(id),
    
    -- Cleaned fields
    event_id VARCHAR(100) NOT NULL,
    student_id VARCHAR(20),
    course_id VARCHAR(20),
    event_type VARCHAR(50),
    event_timestamp TIMESTAMP WITH TIME ZONE,
    duration_seconds INTEGER,
    score DECIMAL(5, 2),
    module_id VARCHAR(50),
    completion_percentage DECIMAL(5, 2),
    
    -- Quality flags
    is_student_valid BOOLEAN DEFAULT FALSE,
    is_timestamp_valid BOOLEAN DEFAULT FALSE,
    is_score_valid BOOLEAN DEFAULT FALSE,
    is_duplicate BOOLEAN DEFAULT FALSE,
    quality_score INTEGER DEFAULT 100,
    
    -- Processing metadata
    cleaned_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    batch_id VARCHAR(100),
    
    CONSTRAINT uq_stg_event_id UNIQUE (event_id)
);

CREATE INDEX idx_stg_progress_student ON staging.stg_progress(student_id);
CREATE INDEX idx_stg_progress_timestamp ON staging.stg_progress(event_timestamp);

-- Staging: Cleaned Support Tickets
CREATE TABLE staging.stg_tickets (
    id SERIAL PRIMARY KEY,
    raw_id INTEGER REFERENCES raw.support_tickets(id),
    
    -- Cleaned fields
    ticket_id VARCHAR(50) NOT NULL,
    student_id VARCHAR(20),
    subject VARCHAR(500),
    description TEXT,
    priority VARCHAR(20),
    status VARCHAR(20),
    category VARCHAR(100),
    created_date DATE,
    resolved_date DATE,
    
    -- AI Enrichment fields
    ai_sentiment VARCHAR(20),
    ai_sentiment_score DECIMAL(5, 4),
    ai_category_suggestion VARCHAR(100),
    ai_priority_suggestion VARCHAR(20),
    
    -- Quality flags
    is_student_valid BOOLEAN DEFAULT FALSE,
    quality_score INTEGER DEFAULT 100,
    
    -- Processing metadata
    cleaned_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    ai_processed_at TIMESTAMP WITH TIME ZONE,
    batch_id VARCHAR(100),
    
    CONSTRAINT uq_stg_ticket_id UNIQUE (ticket_id)
);

-- Staging: Quality Audit Log
CREATE TABLE staging.stg_quality_log (
    id SERIAL PRIMARY KEY,
    source_table VARCHAR(100) NOT NULL,
    record_id INTEGER NOT NULL,
    original_value TEXT,
    cleaned_value TEXT,
    rule_applied VARCHAR(100),
    is_valid BOOLEAN,
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    batch_id VARCHAR(100)
);

CREATE INDEX idx_quality_log_source ON staging.stg_quality_log(source_table, record_id);

-- ============================================
-- WAREHOUSE LAYER
-- Dimensional Model (Star Schema)
-- ============================================

-- Dimension: Date
CREATE TABLE warehouse.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    day_of_month INTEGER NOT NULL,
    week_of_year INTEGER NOT NULL,
    month_number INTEGER NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    quarter INTEGER NOT NULL,
    year INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- Dimension: Students
CREATE TABLE warehouse.dim_students (
    student_sk SERIAL PRIMARY KEY,
    student_id VARCHAR(20) NOT NULL,
    
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(20),
    dob DATE,
    age INTEGER,
    age_group VARCHAR(20),
    gender VARCHAR(10),
    
    city VARCHAR(100),
    state VARCHAR(100),
    
    enrollment_date DATE,
    enrollment_month INTEGER,
    enrollment_year INTEGER,
    enrollment_quarter INTEGER,
    
    program_id VARCHAR(20),
    fee_paid DECIMAL(15, 2),
    payment_status VARCHAR(20),
    
    -- AI Enrichment
    ai_risk_score DECIMAL(5, 2),
    ai_risk_category VARCHAR(20),
    ai_churn_probability DECIMAL(5, 4),
    
    -- Derived fields
    total_courses_enrolled INTEGER DEFAULT 0,
    total_time_spent_hours DECIMAL(10, 2) DEFAULT 0,
    avg_score DECIMAL(5, 2),
    last_activity_date DATE,
    days_since_last_activity INTEGER,
    enrollment_status VARCHAR(20) DEFAULT 'PENDING',
    
    -- Metadata
    quality_score INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT uq_dim_student_id UNIQUE (student_id)
);

CREATE INDEX idx_dim_students_enrollment ON warehouse.dim_students(enrollment_status);
CREATE INDEX idx_dim_students_risk ON warehouse.dim_students(ai_risk_category);

-- Dimension: Courses
CREATE TABLE warehouse.dim_courses (
    course_sk SERIAL PRIMARY KEY,
    course_id VARCHAR(20) NOT NULL UNIQUE,
    
    course_name VARCHAR(255),
    category VARCHAR(100),
    difficulty VARCHAR(50),
    duration_hours INTEGER,
    price DECIMAL(15, 2),
    instructor_name VARCHAR(255),
    is_active BOOLEAN DEFAULT TRUE,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Fact: Student Progress
CREATE TABLE warehouse.fact_student_progress (
    progress_sk BIGSERIAL PRIMARY KEY,
    
    -- Dimension Keys
    student_sk INTEGER REFERENCES warehouse.dim_students(student_sk),
    course_sk INTEGER REFERENCES warehouse.dim_courses(course_sk),
    date_key INTEGER REFERENCES warehouse.dim_date(date_key),
    
    -- Degenerate Dimensions
    event_id VARCHAR(100) NOT NULL,
    event_type VARCHAR(50),
    module_id VARCHAR(50),
    
    -- Measures
    duration_seconds INTEGER,
    score DECIMAL(5, 2),
    completion_percentage DECIMAL(5, 2),
    
    -- Timestamp
    event_timestamp TIMESTAMP WITH TIME ZONE,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT uq_fact_progress_event UNIQUE (event_id)
);

CREATE INDEX idx_fact_progress_student ON warehouse.fact_student_progress(student_sk);
CREATE INDEX idx_fact_progress_date ON warehouse.fact_student_progress(date_key);

-- Fact: Enrollments
CREATE TABLE warehouse.fact_enrollments (
    enrollment_sk SERIAL PRIMARY KEY,
    
    -- Dimension Keys
    student_sk INTEGER REFERENCES warehouse.dim_students(student_sk),
    course_sk INTEGER REFERENCES warehouse.dim_courses(course_sk),
    enrollment_date_key INTEGER REFERENCES warehouse.dim_date(date_key),
    
    -- Measures
    fee_paid DECIMAL(15, 2),
    
    -- Status
    payment_status VARCHAR(20),
    enrollment_status VARCHAR(20),
    
    -- Progress Summary (updated periodically)
    total_time_spent_minutes INTEGER DEFAULT 0,
    modules_completed INTEGER DEFAULT 0,
    avg_score DECIMAL(5, 2),
    completion_percentage DECIMAL(5, 2) DEFAULT 0,
    last_activity_date DATE,
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT uq_fact_enrollment UNIQUE (student_sk, course_sk)
);

-- Fact: Support Tickets
CREATE TABLE warehouse.fact_support_tickets (
    ticket_sk SERIAL PRIMARY KEY,
    
    -- Dimension Keys
    student_sk INTEGER REFERENCES warehouse.dim_students(student_sk),
    created_date_key INTEGER REFERENCES warehouse.dim_date(date_key),
    resolved_date_key INTEGER REFERENCES warehouse.dim_date(date_key),
    
    -- Degenerate Dimensions
    ticket_id VARCHAR(50) NOT NULL,
    category VARCHAR(100),
    priority VARCHAR(20),
    
    -- Measures
    resolution_time_hours DECIMAL(10, 2),
    
    -- Status
    status VARCHAR(20),
    
    -- AI Enrichment
    ai_sentiment VARCHAR(20),
    ai_sentiment_score DECIMAL(5, 4),
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE,
    resolved_at TIMESTAMP WITH TIME ZONE,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT uq_fact_ticket UNIQUE (ticket_id)
);

-- Fact: Daily Metrics (Aggregated)
CREATE TABLE warehouse.fact_daily_metrics (
    id SERIAL PRIMARY KEY,
    date_key INTEGER REFERENCES warehouse.dim_date(date_key),
    
    -- Student Metrics
    total_active_students INTEGER DEFAULT 0,
    new_enrollments INTEGER DEFAULT 0,
    students_at_risk INTEGER DEFAULT 0,
    
    -- Progress Metrics
    total_events INTEGER DEFAULT 0,
    total_time_spent_hours DECIMAL(10, 2) DEFAULT 0,
    avg_completion_rate DECIMAL(5, 2),
    
    -- Support Metrics
    new_tickets INTEGER DEFAULT 0,
    resolved_tickets INTEGER DEFAULT 0,
    avg_resolution_hours DECIMAL(10, 2),
    negative_sentiment_count INTEGER DEFAULT 0,
    
    -- Quality Metrics
    avg_data_quality_score DECIMAL(5, 2),
    records_processed INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT uq_fact_daily_date UNIQUE (date_key)
);

-- ============================================
-- ANALYTICS LAYER
-- Pre-aggregated views
-- ============================================

-- View: Student 360 (Complete student view)
CREATE VIEW analytics.v_student_360 AS
SELECT 
    s.student_id,
    s.full_name,
    s.email,
    s.phone,
    s.age,
    s.age_group,
    s.city,
    s.state,
    s.enrollment_date,
    s.enrollment_status,
    s.payment_status,
    s.fee_paid,
    s.total_courses_enrolled,
    s.total_time_spent_hours,
    s.avg_score,
    s.last_activity_date,
    s.days_since_last_activity,
    s.ai_risk_score,
    s.ai_risk_category,
    s.quality_score,
    -- Ticket summary
    (SELECT COUNT(*) FROM warehouse.fact_support_tickets t WHERE t.student_sk = s.student_sk) as total_tickets,
    (SELECT COUNT(*) FROM warehouse.fact_support_tickets t WHERE t.student_sk = s.student_sk AND t.status = 'Open') as open_tickets
FROM warehouse.dim_students s;

-- View: Course Performance
CREATE VIEW analytics.v_course_performance AS
SELECT 
    c.course_id,
    c.course_name,
    c.category,
    c.difficulty,
    c.price,
    COUNT(DISTINCT e.student_sk) as total_enrollments,
    SUM(e.fee_paid) as total_revenue,
    AVG(e.completion_percentage) as avg_completion_rate,
    AVG(e.avg_score) as avg_student_score,
    SUM(e.total_time_spent_minutes) / 60.0 as total_hours_spent
FROM warehouse.dim_courses c
LEFT JOIN warehouse.fact_enrollments e ON c.course_sk = e.course_sk
GROUP BY c.course_sk, c.course_id, c.course_name, c.category, c.difficulty, c.price;

-- View: Daily Dashboard
CREATE VIEW analytics.v_daily_dashboard AS
SELECT 
    d.full_date,
    d.day_name,
    d.month_name,
    d.year,
    m.total_active_students,
    m.new_enrollments,
    m.students_at_risk,
    m.total_events,
    m.total_time_spent_hours,
    m.new_tickets,
    m.resolved_tickets,
    m.negative_sentiment_count,
    m.avg_data_quality_score
FROM warehouse.fact_daily_metrics m
JOIN warehouse.dim_date d ON m.date_key = d.date_key
ORDER BY d.full_date DESC;

-- View: AI Insights Summary
CREATE VIEW analytics.v_ai_insights AS
SELECT 
    'High Risk Students' as metric_name,
    COUNT(*) as metric_value,
    'Students with risk score > 70' as description
FROM warehouse.dim_students 
WHERE ai_risk_score > 70

UNION ALL

SELECT 
    'Negative Sentiment Tickets',
    COUNT(*),
    'Tickets with negative sentiment'
FROM warehouse.fact_support_tickets 
WHERE ai_sentiment = 'Negative' OR ai_sentiment = 'Very Negative'

UNION ALL

SELECT 
    'Low Quality Records',
    COUNT(*),
    'Student records with quality score < 60'
FROM warehouse.dim_students 
WHERE quality_score < 60;

-- ============================================
-- METADATA TABLES
-- ============================================

-- Pipeline Run Tracking
CREATE TABLE metadata.pipeline_runs (
    run_id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    dag_id VARCHAR(100),
    
    start_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    end_time TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) DEFAULT 'RUNNING',
    
    records_read INTEGER DEFAULT 0,
    records_written INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    
    input_file VARCHAR(255),
    error_message TEXT,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Data Quality Summary
CREATE TABLE metadata.data_quality_summary (
    id SERIAL PRIMARY KEY,
    run_id UUID REFERENCES metadata.pipeline_runs(run_id),
    
    table_name VARCHAR(100) NOT NULL,
    total_records INTEGER,
    valid_records INTEGER,
    invalid_records INTEGER,
    duplicate_records INTEGER,
    
    avg_quality_score DECIMAL(5, 2),
    
    rule_stats JSONB, -- {"email_invalid": 5, "phone_invalid": 3, ...}
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- AI Processing Log
CREATE TABLE metadata.ai_processing_log (
    id SERIAL PRIMARY KEY,
    run_id UUID REFERENCES metadata.pipeline_runs(run_id),
    
    agent_name VARCHAR(100) NOT NULL,
    records_processed INTEGER,
    processing_time_seconds DECIMAL(10, 2),
    tokens_used INTEGER,
    
    status VARCHAR(20),
    error_message TEXT,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ============================================
-- POPULATE DATE DIMENSION
-- ============================================

INSERT INTO warehouse.dim_date (date_key, full_date, day_of_week, day_name, day_of_month, 
                                 week_of_year, month_number, month_name, quarter, year, is_weekend)
SELECT 
    TO_CHAR(d, 'YYYYMMDD')::INTEGER as date_key,
    d as full_date,
    EXTRACT(DOW FROM d)::INTEGER as day_of_week,
    TRIM(TO_CHAR(d, 'Day')) as day_name,
    EXTRACT(DAY FROM d)::INTEGER as day_of_month,
    EXTRACT(WEEK FROM d)::INTEGER as week_of_year,
    EXTRACT(MONTH FROM d)::INTEGER as month_number,
    TRIM(TO_CHAR(d, 'Month')) as month_name,
    EXTRACT(QUARTER FROM d)::INTEGER as quarter,
    EXTRACT(YEAR FROM d)::INTEGER as year,
    EXTRACT(DOW FROM d) IN (0, 6) as is_weekend
FROM generate_series('2020-01-01'::DATE, '2026-12-31'::DATE, '1 day'::INTERVAL) d
ON CONFLICT (date_key) DO NOTHING;

-- ============================================
-- HELPER FUNCTION: Get Date Key
-- ============================================

CREATE OR REPLACE FUNCTION warehouse.get_date_key(input_date DATE)
RETURNS INTEGER AS $$
BEGIN
    RETURN TO_CHAR(input_date, 'YYYYMMDD')::INTEGER;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ============================================
-- COMMENTS FOR DOCUMENTATION
-- ============================================

COMMENT ON SCHEMA raw IS 'Raw data layer - exact copy of source files';
COMMENT ON SCHEMA staging IS 'Staging layer - after cleaning with quality flags';
COMMENT ON SCHEMA warehouse IS 'Data warehouse - dimensional model';
COMMENT ON SCHEMA analytics IS 'Analytics layer - pre-aggregated views';
COMMENT ON SCHEMA metadata IS 'Pipeline and quality tracking metadata';

COMMENT ON TABLE staging.stg_students IS 'Cleaned student records with quality scores';
COMMENT ON TABLE warehouse.dim_students IS 'Student dimension with AI enrichment';
COMMENT ON TABLE warehouse.fact_student_progress IS 'Student learning events fact table';
