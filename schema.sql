PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS gas_price_daily (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date TEXT NOT NULL,
    hub TEXT NOT NULL,
    price_value REAL,
    price_unit TEXT NOT NULL,
    source_key TEXT NOT NULL,
    source_name TEXT NOT NULL,
    source_url TEXT,
    fetched_at TEXT NOT NULL,
    raw_payload TEXT,
    status TEXT NOT NULL,
    is_real_data INTEGER NOT NULL DEFAULT 0,
    checksum TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_gas_price_daily_checksum
ON gas_price_daily (checksum);

CREATE INDEX IF NOT EXISTS idx_gas_price_daily_trade_date
ON gas_price_daily (trade_date DESC);

CREATE INDEX IF NOT EXISTS idx_gas_price_daily_source_key
ON gas_price_daily (source_key);

CREATE INDEX IF NOT EXISTS idx_gas_price_daily_status
ON gas_price_daily (status);

CREATE TABLE IF NOT EXISTS fetch_job_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_name TEXT NOT NULL,
    source_key TEXT,
    source_name TEXT,
    run_at TEXT NOT NULL,
    result TEXT NOT NULL,
    message TEXT,
    duration_ms INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fetch_job_log_run_at
ON fetch_job_log (run_at DESC);

CREATE TABLE IF NOT EXISTS alert_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_type TEXT NOT NULL,
    level TEXT NOT NULL,
    trade_date TEXT,
    message TEXT NOT NULL,
    source_key TEXT,
    source_name TEXT,
    resolved INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_alert_records_created_at
ON alert_records (created_at DESC);

CREATE TABLE IF NOT EXISTS system_settings (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    primary_source TEXT NOT NULL,
    backup_source TEXT NOT NULL,
    run_time TEXT NOT NULL,
    diff_threshold_percent REAL NOT NULL,
    retention_policy TEXT NOT NULL,
    enable_validation INTEGER NOT NULL DEFAULT 0,
    enable_alert INTEGER NOT NULL DEFAULT 1,
    enable_archive INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL
);
