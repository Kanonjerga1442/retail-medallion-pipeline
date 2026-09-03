-- ============================================================
-- 1. DATABASE RIÊNG CHO METABASE
-- ============================================================

SELECT 'CREATE DATABASE metabase_app'
WHERE NOT EXISTS (
    SELECT FROM pg_database
    WHERE datname = 'metabase_app'
)
\gexec


-- ============================================================
-- 2. CHUYỂN VỀ DATABASE WAREHOUSE
-- ============================================================

\connect warehouse


-- ============================================================
-- 3. TẠO CÁC SCHEMA CHO DATA PIPELINE
-- ============================================================

CREATE SCHEMA IF NOT EXISTS control;
CREATE SCHEMA IF NOT EXISTS temp;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS audit;


-- ============================================================
-- 4. WATERMARK
-- ============================================================

CREATE TABLE IF NOT EXISTS control.pipeline_watermark (
    pipeline_name VARCHAR(100) PRIMARY KEY,
    last_success_date DATE,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);


INSERT INTO control.pipeline_watermark (
    pipeline_name,
    last_success_date
)
VALUES (
    'online_retail',
    DATE '2009-11-30'
)
ON CONFLICT (pipeline_name) DO NOTHING;


-- ============================================================
-- 5. BẢNG LƯU KẾT QUẢ RECONCILIATION
-- ============================================================

CREATE TABLE IF NOT EXISTS audit.reconciliation_result (
    id BIGSERIAL PRIMARY KEY,

    run_id VARCHAR(200),

    process_date DATE,

    source_layer VARCHAR(50),
    target_layer VARCHAR(50),

    reconcile_type VARCHAR(50),

    source_value NUMERIC,
    target_value NUMERIC,
    difference NUMERIC,

    status VARCHAR(20),

    details TEXT,

    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
