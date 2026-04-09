import json
from collections import Counter
from datetime import date, datetime, timedelta

from app.config import DEFAULT_SETTINGS, EXPORT_FIELDS, SOURCE_OPTIONS
from app.db import get_connection, utc_now_iso


STATUS_PRIORITY = {
    "pending_review": 4,
    "normal": 3,
    "mock_fallback": 2,
    "fetch_failed": 1,
}


def _normalize_bool(value):
    return 1 if value else 0


def ensure_default_settings():
    now = utc_now_iso()
    with get_connection() as connection:
        connection.execute(
            """
            INSERT OR IGNORE INTO system_settings (
                id,
                primary_source,
                backup_source,
                run_time,
                diff_threshold_percent,
                retention_policy,
                enable_validation,
                enable_alert,
                enable_archive,
                updated_at
            ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                DEFAULT_SETTINGS["primary_source"],
                DEFAULT_SETTINGS["backup_source"],
                DEFAULT_SETTINGS["run_time"],
                DEFAULT_SETTINGS["diff_threshold_percent"],
                DEFAULT_SETTINGS["retention_policy"],
                _normalize_bool(DEFAULT_SETTINGS["enable_validation"]),
                _normalize_bool(DEFAULT_SETTINGS["enable_alert"]),
                _normalize_bool(DEFAULT_SETTINGS["enable_archive"]),
                now,
            ),
        )
        connection.commit()


def get_settings():
    ensure_default_settings()
    with get_connection() as connection:
        row = connection.execute("SELECT * FROM system_settings WHERE id = 1").fetchone()
    return {
        "primary_source": row["primary_source"],
        "backup_source": row["backup_source"],
        "run_time": row["run_time"],
        "diff_threshold_percent": float(row["diff_threshold_percent"]),
        "retention_policy": row["retention_policy"],
        "enable_validation": bool(row["enable_validation"]),
        "enable_alert": bool(row["enable_alert"]),
        "enable_archive": bool(row["enable_archive"]),
        "updated_at": row["updated_at"],
        "available_sources": SOURCE_OPTIONS,
    }


def save_settings(payload):
    current = get_settings()
    merged = {**current, **payload}
    now = utc_now_iso()
    with get_connection() as connection:
        connection.execute(
            """
            UPDATE system_settings
            SET primary_source = ?,
                backup_source = ?,
                run_time = ?,
                diff_threshold_percent = ?,
                retention_policy = ?,
                enable_validation = ?,
                enable_alert = ?,
                enable_archive = ?,
                updated_at = ?
            WHERE id = 1
            """,
            (
                merged["primary_source"],
                merged["backup_source"],
                merged["run_time"],
                float(merged["diff_threshold_percent"]),
                merged["retention_policy"],
                _normalize_bool(merged["enable_validation"]),
                _normalize_bool(merged["enable_alert"]),
                _normalize_bool(merged["enable_archive"]),
                now,
            ),
        )
        connection.commit()
    return get_settings()


def insert_price_records(records):
    if not records:
        return 0

    inserted = 0
    with get_connection() as connection:
        for record in records:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO gas_price_daily (
                    trade_date,
                    hub,
                    price_value,
                    price_unit,
                    source_key,
                    source_name,
                    source_url,
                    fetched_at,
                    raw_payload,
                    status,
                    is_real_data,
                    checksum,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record["trade_date"],
                    record["hub"],
                    record["price_value"],
                    record["price_unit"],
                    record["source_key"],
                    record["source_name"],
                    record.get("source_url", ""),
                    record["fetched_at"],
                    json.dumps(record.get("raw_payload", {}), ensure_ascii=False),
                    record["status"],
                    1 if record["is_real_data"] else 0,
                    record["checksum"],
                    record["created_at"],
                    record["updated_at"],
                ),
            )
            inserted += cursor.rowcount
        connection.commit()
    return inserted


def insert_job_log(job_name, source_key, source_name, result, message, duration_ms):
    now = utc_now_iso()
    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO fetch_job_log (
                job_name,
                source_key,
                source_name,
                run_at,
                result,
                message,
                duration_ms,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (job_name, source_key, source_name, now, result, message, int(duration_ms), now),
        )
        connection.commit()


def insert_alert(alert_type, level, message, trade_date=None, source_key=None, source_name=None):
    now = utc_now_iso()
    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO alert_records (
                alert_type,
                level,
                trade_date,
                message,
                source_key,
                source_name,
                resolved,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, 0, ?)
            """,
            (alert_type, level, trade_date, message, source_key, source_name, now),
        )
        connection.commit()


def get_recent_logs(limit=20):
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT id, job_name, source_key, source_name, run_at, result, message, duration_ms
            FROM fetch_job_log
            ORDER BY run_at DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def get_recent_alerts(limit=20):
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT id, alert_type, level, trade_date, message, source_key, source_name, resolved, created_at
            FROM alert_records
            ORDER BY resolved ASC, created_at DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def has_any_price_records():
    with get_connection() as connection:
        row = connection.execute("SELECT COUNT(*) AS count FROM gas_price_daily").fetchone()
    return row["count"] > 0


def _build_filters(filters, params):
    clauses = []

    keyword = (filters.get("keyword") or "").strip()
    if keyword:
        clauses.append(
            """
            (
                lower(trade_date) LIKE lower(?)
                OR lower(hub) LIKE lower(?)
                OR lower(source_name) LIKE lower(?)
                OR lower(status) LIKE lower(?)
                OR lower(price_unit) LIKE lower(?)
            )
            """
        )
        token = f"%{keyword}%"
        params.extend([token, token, token, token, token])

    if filters.get("start_date"):
        clauses.append("trade_date >= ?")
        params.append(filters["start_date"])

    if filters.get("end_date"):
        clauses.append("trade_date <= ?")
        params.append(filters["end_date"])

    if filters.get("source"):
        clauses.append("source_key = ?")
        params.append(filters["source"])

    if filters.get("status"):
        clauses.append("status = ?")
        params.append(filters["status"])

    return " AND ".join(clauses)


def _canonical_cte(filters):
    preferred_source = get_settings()["primary_source"]
    params = []
    where_sql = _build_filters(filters, params)
    if where_sql:
        where_sql = f"WHERE {where_sql}"

    cte = f"""
        WITH ranked AS (
            SELECT
                *,
                CASE
                    WHEN is_real_data = 1 THEN 'real'
                    ELSE 'mock'
                END AS data_kind,
                ROW_NUMBER() OVER (
                    PARTITION BY trade_date, hub
                    ORDER BY
                        is_real_data DESC,
                        CASE
                            WHEN source_key = ? THEN 2
                            WHEN is_real_data = 1 THEN 1
                            ELSE 0
                        END DESC,
                        CASE status
                            WHEN 'pending_review' THEN 4
                            WHEN 'normal' THEN 3
                            WHEN 'mock_fallback' THEN 2
                            WHEN 'fetch_failed' THEN 1
                            ELSE 0
                        END DESC,
                        fetched_at DESC,
                        id DESC
                ) AS rn
            FROM gas_price_daily
            {where_sql}
        )
    """
    return cte, [preferred_source] + params


def get_history(filters, page=1, page_size=25, export_scope="filtered"):
    active_filters = filters.copy()
    if export_scope == "all":
        active_filters = {}

    cte, params = _canonical_cte(active_filters)
    offset = max(page - 1, 0) * page_size

    query = (
        cte
        + """
        SELECT
            id,
            trade_date,
            hub,
            ROUND(price_value, 3) AS price,
            price_unit AS unit,
            source_key,
            source_name AS source,
            source_url,
            fetched_at,
            status,
            is_real_data,
            data_kind
        FROM ranked
        WHERE rn = 1
        ORDER BY trade_date DESC, id DESC
        LIMIT ? OFFSET ?
        """
    )
    query_params = params + [page_size, offset]

    count_query = cte + "SELECT COUNT(*) AS count FROM ranked WHERE rn = 1"

    with get_connection() as connection:
        rows = connection.execute(query, query_params).fetchall()
        total = connection.execute(count_query, params).fetchone()["count"]

    items = [dict(row) for row in rows]
    return {
        "items": items,
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": max((total + page_size - 1) // page_size, 1),
        },
    }


def get_history_for_export(filters, export_scope):
    data = get_history(filters, page=1, page_size=100000, export_scope=export_scope)
    return data["items"]


def get_latest_record():
    cte, params = _canonical_cte({})
    query = (
        cte
        + """
        SELECT
            trade_date,
            hub,
            ROUND(price_value, 3) AS price,
            price_unit AS unit,
            source_key,
            source_name AS source,
            source_url,
            fetched_at,
            status,
            is_real_data,
            data_kind
        FROM ranked
        WHERE rn = 1
        ORDER BY trade_date DESC, id DESC
        LIMIT 1
        """
    )
    with get_connection() as connection:
        row = connection.execute(query, params).fetchone()
    return dict(row) if row else None


def get_history_window(start_date, end_date):
    cte, params = _canonical_cte({"start_date": start_date, "end_date": end_date})
    query = (
        cte
        + """
        SELECT
            trade_date,
            hub,
            ROUND(price_value, 3) AS price,
            price_unit AS unit,
            source_key,
            source_name AS source,
            source_url,
            fetched_at,
            status,
            is_real_data,
            data_kind
        FROM ranked
        WHERE rn = 1
        ORDER BY trade_date ASC, id ASC
        """
    )
    with get_connection() as connection:
        rows = connection.execute(query, params).fetchall()
    return [dict(row) for row in rows]


def get_dashboard_summary():
    settings = get_settings()
    latest = get_latest_record()
    if latest:
        window_end = date.fromisoformat(latest["trade_date"])
    else:
        window_end = date.today()
    start_90 = (window_end - timedelta(days=89)).isoformat()
    window_rows = get_history_window(start_90, window_end.isoformat())

    real_days = sum(1 for row in window_rows if row["data_kind"] == "real")
    mock_days = sum(1 for row in window_rows if row["data_kind"] != "real")
    status_counts = Counter(row["status"] for row in window_rows)

    with get_connection() as connection:
        archive_total = connection.execute("SELECT COUNT(*) AS count FROM gas_price_daily").fetchone()["count"]
        preferred_source = settings["primary_source"]
        canonical_total = connection.execute(
            """
            WITH ranked AS (
                SELECT
                    trade_date,
                    hub,
                    ROW_NUMBER() OVER (
                        PARTITION BY trade_date, hub
                        ORDER BY
                            is_real_data DESC,
                            CASE
                                WHEN source_key = ? THEN 2
                                WHEN is_real_data = 1 THEN 1
                                ELSE 0
                            END DESC,
                            fetched_at DESC,
                            id DESC
                    ) AS rn
                FROM gas_price_daily
            )
            SELECT COUNT(*) AS count FROM ranked WHERE rn = 1
            """,
            (preferred_source,),
        ).fetchone()["count"]
        last_job = connection.execute(
            """
            SELECT job_name, source_name, source_key, run_at, result, message
            FROM fetch_job_log
            ORDER BY run_at DESC, id DESC
            LIMIT 1
            """
        ).fetchone()

    return {
        "latest": latest,
        "retention_policy": settings["retention_policy"],
        "archive_total": archive_total,
        "canonical_total": canonical_total,
        "window_real_days": real_days,
        "window_mock_days": mock_days,
        "status_counts": dict(status_counts),
        "last_job": dict(last_job) if last_job else None,
    }


def get_missing_trade_dates(start_date, end_date):
    if isinstance(start_date, str):
        start_date = date.fromisoformat(start_date)
    if isinstance(end_date, str):
        end_date = date.fromisoformat(end_date)

    with get_connection() as connection:
        preferred_source = get_settings()["primary_source"]
        rows = connection.execute(
            """
            WITH ranked AS (
                SELECT
                    trade_date,
                    hub,
                    ROW_NUMBER() OVER (
                        PARTITION BY trade_date, hub
                        ORDER BY
                            is_real_data DESC,
                            CASE
                                WHEN source_key = ? THEN 2
                                WHEN is_real_data = 1 THEN 1
                                ELSE 0
                            END DESC,
                            fetched_at DESC,
                            id DESC
                    ) AS rn
                FROM gas_price_daily
            )
            SELECT trade_date FROM ranked
            WHERE rn = 1 AND trade_date BETWEEN ? AND ?
            """,
            (preferred_source, start_date.isoformat(), end_date.isoformat()),
        ).fetchall()

    existing_dates = {date.fromisoformat(row["trade_date"]) for row in rows}
    missing = []
    cursor = start_date
    while cursor <= end_date:
        if cursor.weekday() < 5 and cursor not in existing_dates:
            missing.append(cursor)
        cursor += timedelta(days=1)
    return missing


def purge_future_mock_records(cutoff_date):
    if isinstance(cutoff_date, date):
        cutoff_value = cutoff_date.isoformat()
    else:
        cutoff_value = cutoff_date

    with get_connection() as connection:
        cursor = connection.execute(
            """
            DELETE FROM gas_price_daily
            WHERE source_key = 'mock_fallback' AND trade_date > ?
            """,
            (cutoff_value,),
        )
        connection.commit()
    return cursor.rowcount
