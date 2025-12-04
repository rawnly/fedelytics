-- ============================================
-- DATABASES
-- ============================================

CREATE DATABASE IF NOT EXISTS projects;
CREATE DATABASE IF NOT EXISTS telemetry;

-- ============================================
-- PROJECTS TABLE
-- Every CLI/tool that sends telemetry gets one row.
-- ============================================

CREATE TABLE IF NOT EXISTS projects.list (
    project_id    UUID          DEFAULT generateUUIDv4(),
    name          String,       -- e.g. "splash-cli"
    owner         String,       -- your email or org
    created_at    DateTime      DEFAULT now(),
    description   String        DEFAULT ''
)
ENGINE = MergeTree()
ORDER BY (name);

-- Optional: unique index to prevent duplicates
ALTER TABLE projects.list
    ADD CONSTRAINT IF NOT EXISTS unique_project_name
    CHECK name != '' AND length(name) > 0;


-- ============================================
-- TELEMETRY EVENTS
-- Generic, reusable, append-only
-- ============================================

CREATE TABLE IF NOT EXISTS telemetry.events (
    event_time    DateTime      DEFAULT now(),
    project_id    UUID,             -- FK to projects.list
    user_id       String,           -- pseudo anon or hashed
    session_id    UUID DEFAULT generateUUIDv4(),

    cli_version   LowCardinality(String),
    command       LowCardinality(String),
    exit_code     Int32,

    os            LowCardinality(String),
    arch          LowCardinality(String),

    latency_ms    Float32        DEFAULT 0,
    success       UInt8          DEFAULT 1,

    extra         JSON,           -- anything else

    ingested_at   DateTime        DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (project_id, event_time);


-- ============================================
-- TELEMETRY: DAILY AGGREGATES (OPTIONAL)
-- For dashboarding / fast queries
-- ============================================

CREATE TABLE IF NOT EXISTS telemetry.daily_usage (
    day           Date,
    project_id    UUID,
    command       LowCardinality(String),
    count         UInt64,
    users         UInt64
)
ENGINE = AggregatingMergeTree()
ORDER BY (project_id, day, command);


-- ============================================
-- MATERIALIZED VIEW to auto-populate aggregates
-- ============================================

CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry.mv_daily_usage
TO telemetry.daily_usage
AS
SELECT
    toDate(event_time) AS day,
    project_id,
    command,
    count() AS count,
    uniqExact(user_id) AS users
FROM telemetry.events
GROUP BY day, project_id, command;
