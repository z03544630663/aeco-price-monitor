import time
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from app.config import APP_TIMEZONE
from app.repository import (
    get_dashboard_summary,
    get_missing_trade_dates,
    purge_future_mock_records,
    get_settings,
    insert_alert,
    insert_job_log,
    insert_price_records,
)
from app.services.sources import MockFallbackAdapter, SourceError, get_source_adapter


def _app_today():
    return datetime.now(ZoneInfo(APP_TIMEZONE)).date()


def _record_alert(settings, alert_type, level, message, trade_date=None, source_key=None, source_name=None):
    if not settings.get("enable_alert", True):
        return
    insert_alert(
        alert_type,
        level,
        message,
        trade_date=trade_date,
        source_key=source_key,
        source_name=source_name,
    )


def _build_review_records(primary_records, backup_records, threshold_percent):
    backup_map = {
        (row["trade_date"], row["hub"]): row for row in backup_records if row.get("price_value") is not None
    }
    review_records = []
    alerts = []

    for primary in primary_records:
        key = (primary["trade_date"], primary["hub"])
        backup = backup_map.get(key)
        if not backup:
            continue
        if primary["price_unit"] != backup["price_unit"]:
            alerts.append(
                {
                    "type": "unit_mismatch",
                    "level": "warning",
                    "trade_date": primary["trade_date"],
                    "source_key": backup["source_key"],
                    "source_name": backup["source_name"],
                    "message": f"{primary['trade_date']} 备源单位为 {backup['price_unit']}，与主源 {primary['price_unit']} 不一致，跳过校验。",
                }
            )
            continue

        primary_price = float(primary["price_value"])
        backup_price = float(backup["price_value"])
        if primary_price == 0:
            continue

        diff_percent = abs(primary_price - backup_price) / primary_price * 100
        if diff_percent <= threshold_percent:
            continue

        review = dict(primary)
        review["status"] = "pending_review"
        review["raw_payload"] = {
            **(primary.get("raw_payload") or {}),
            "validation": {
                "backup_source": backup["source_name"],
                "backup_price": backup_price,
                "diff_percent": round(diff_percent, 3),
            },
        }
        from app.services.sources import make_checksum

        review["checksum"] = make_checksum(
            {
                "trade_date": review["trade_date"],
                "hub": review["hub"],
                "price_value": review["price_value"],
                "price_unit": review["price_unit"],
                "source_key": review["source_key"],
                "status": review["status"],
                "backup_source": backup["source_key"],
                "diff_percent": round(diff_percent, 3),
            }
        )
        review_records.append(review)
        alerts.append(
            {
                "type": "source_diff_pending_review",
                "level": "warning",
                "trade_date": review["trade_date"],
                "source_key": review["source_key"],
                "source_name": review["source_name"],
                "message": f"{review['trade_date']} 双源差异 {diff_percent:.2f}% 超过阈值 {threshold_percent:.2f}%，已标记待复核。",
            }
        )
    return review_records, alerts


def run_fetch_job(trigger="manual"):
    started_at = time.perf_counter()
    settings = get_settings()
    reference_date = _app_today()
    primary_key = settings["primary_source"]
    backup_key = settings["backup_source"]

    primary_adapter = get_source_adapter(primary_key)
    summary = {
        "trigger": trigger,
        "reference_date": reference_date.isoformat(),
        "primary_source": primary_key,
        "backup_source": backup_key,
        "inserted_real_records": 0,
        "inserted_mock_records": 0,
        "review_records": 0,
        "result": "success",
        "message": "",
    }

    try:
        primary_result = primary_adapter.fetch_recent(reference_date)
        inserted_real = insert_price_records(primary_result.records)
        summary["inserted_real_records"] += inserted_real
        insert_job_log(
            "fetch_primary",
            primary_key,
            primary_adapter.name,
            "success",
            f"Fetched {len(primary_result.records)} records from public source.",
            (time.perf_counter() - started_at) * 1000,
        )

        if not primary_result.records:
            summary["result"] = "partial"
            _record_alert(
                settings,
                "no_primary_records",
                "warning",
                "主源抓取完成但未返回可入库的 AECO 日度记录。",
                source_key=primary_key,
                source_name=primary_adapter.name,
            )

        backup_records = []
        if settings["enable_validation"] and backup_key and backup_key != primary_key:
            backup_adapter = get_source_adapter(backup_key)
            if hasattr(backup_adapter, "fetch_recent") and backup_key != "mock_fallback":
                try:
                    backup_result = backup_adapter.fetch_recent(reference_date)
                    backup_records = backup_result.records
                    inserted_backup = insert_price_records(backup_records)
                    insert_job_log(
                        "fetch_backup",
                        backup_key,
                        backup_adapter.name,
                        "success",
                        f"Fetched {len(backup_records)} backup records.",
                        (time.perf_counter() - started_at) * 1000,
                    )
                    summary["inserted_real_records"] += inserted_backup
                except SourceError as backup_error:
                    insert_job_log(
                        "fetch_backup",
                        backup_key,
                        backup_adapter.name,
                        "failure",
                        f"Backup fetch failed: {backup_error}",
                        (time.perf_counter() - started_at) * 1000,
                    )
                    _record_alert(
                        settings,
                        "backup_fetch_failed",
                        "warning",
                        f"备源抓取失败：{backup_error}",
                        source_key=backup_key,
                        source_name=backup_adapter.name,
                    )
            else:
                insert_job_log(
                    "validate_sources",
                    backup_key,
                    backup_adapter.name,
                    "skipped",
                    "Backup source does not support compatible validation in the current version.",
                    (time.perf_counter() - started_at) * 1000,
                )

        if settings["enable_validation"] and backup_records:
            review_records, review_alerts = _build_review_records(
                primary_result.records, backup_records, settings["diff_threshold_percent"]
            )
            if review_records:
                summary["review_records"] = insert_price_records(review_records)
                for alert in review_alerts:
                    _record_alert(
                        settings,
                        alert["type"],
                        alert["level"],
                        alert["message"],
                        trade_date=alert["trade_date"],
                        source_key=alert["source_key"],
                        source_name=alert["source_name"],
                    )

        latest_real_trade_date = max(
            (date.fromisoformat(record["trade_date"]) for record in primary_result.records),
            default=reference_date,
        )
        purge_future_mock_records(latest_real_trade_date)
        window_start = latest_real_trade_date - timedelta(days=119)
        missing_dates = get_missing_trade_dates(window_start, latest_real_trade_date)
        if missing_dates:
            latest_real = max(
                (record for record in primary_result.records),
                key=lambda record: record["trade_date"],
                default=None,
            )
            anchor_price = latest_real["price_value"] if latest_real else 1.65
            mock_records = MockFallbackAdapter().build_records(missing_dates, anchor_price=anchor_price)
            inserted_mock = insert_price_records(mock_records)
            summary["inserted_mock_records"] += inserted_mock
            if inserted_mock:
                insert_job_log(
                    "mock_backfill",
                    "mock_fallback",
                    "Mock Fallback",
                    "partial",
                    f"Backfilled {inserted_mock} missing trade dates because public AECO history is shorter than 120 days.",
                    (time.perf_counter() - started_at) * 1000,
                )
                _record_alert(
                    settings,
                    "insufficient_real_history",
                    "info",
                    f"真实 AECO 日度公开历史不足 120 天，已用明确标识的 mock fallback 补齐 {inserted_mock} 个交易日。",
                    source_key="mock_fallback",
                    source_name="Mock Fallback",
                )

        duration_ms = int((time.perf_counter() - started_at) * 1000)
        summary["message"] = (
            f"Primary insert={summary['inserted_real_records']}, mock insert={summary['inserted_mock_records']}, "
            f"review={summary['review_records']}."
        )
        insert_job_log(
            "daily_fetch",
            primary_key,
            primary_adapter.name,
            summary["result"],
            summary["message"],
            duration_ms,
        )
        summary["dashboard"] = get_dashboard_summary()
        return summary

    except SourceError as error:
        duration_ms = int((time.perf_counter() - started_at) * 1000)
        summary["result"] = "partial"
        summary["message"] = str(error)
        insert_job_log("daily_fetch", primary_key, primary_adapter.name, "failure", str(error), duration_ms)
        _record_alert(
            settings,
            "fetch_failed",
            "critical",
            f"主源抓取失败：{error}",
            source_key=primary_key,
            source_name=primary_adapter.name,
        )

        reference_date = _app_today()
        window_start = reference_date - timedelta(days=119)
        missing_dates = get_missing_trade_dates(window_start, reference_date)
        mock_records = MockFallbackAdapter().build_records(missing_dates or [reference_date], anchor_price=1.65)
        summary["inserted_mock_records"] += insert_price_records(mock_records)
        insert_job_log(
            "mock_backfill",
            "mock_fallback",
            "Mock Fallback",
            "partial",
            f"Primary source failed, inserted {summary['inserted_mock_records']} mock records to keep the UI runnable.",
            duration_ms,
        )
        summary["dashboard"] = get_dashboard_summary()
        return summary
