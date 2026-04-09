import json
import mimetypes
import os
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from app.config import APP_HOST, APP_PORT, STATIC_DIR
from app.db import init_db
from app.repository import (
    get_dashboard_summary,
    get_history,
    get_history_for_export,
    get_recent_alerts,
    get_recent_logs,
    get_settings,
    has_any_price_records,
    save_settings,
)
from app.services.exporters import rows_to_csv, rows_to_json
from app.services.fetcher import run_fetch_job
from app.services.scheduler import DailyScheduler


mimetypes.add_type("text/babel; charset=utf-8", ".jsx")
mimetypes.add_type("application/javascript; charset=utf-8", ".js")


def parse_query(path):
    parsed = urlparse(path)
    raw_params = parse_qs(parsed.query)
    params = {key: values[-1] for key, values in raw_params.items()}
    return parsed, params


def clean_filter_value(value):
    if value in (None, "", "all"):
        return None
    return value


def parse_history_filters(params):
    return {
        "start_date": clean_filter_value(params.get("start_date")),
        "end_date": clean_filter_value(params.get("end_date")),
        "keyword": clean_filter_value(params.get("keyword")),
        "source": clean_filter_value(params.get("source")),
        "status": clean_filter_value(params.get("status")),
    }


class AecoRequestHandler(BaseHTTPRequestHandler):
    server_version = "AECOPriceMonitor/1.0"

    def log_message(self, format, *args):
        return

    def do_GET(self):
        return self.handle_read(include_body=True)

    def do_HEAD(self):
        return self.handle_read(include_body=False)

    def handle_read(self, include_body):
        parsed, params = parse_query(self.path)
        route = parsed.path

        if route == "/":
            return self.serve_static("index.html", include_body=include_body)
        if route.startswith("/static/"):
            relative_path = route[len("/static/") :]
            return self.serve_static(relative_path, include_body=include_body)

        if route == "/api/prices/latest":
            summary = get_dashboard_summary()
            return self.json_response({"item": summary["latest"], "summary": summary}, include_body=include_body)

        if route == "/api/prices/history":
            page = max(int(params.get("page", "1")), 1)
            page_size = min(max(int(params.get("page_size", "25")), 1), 200)
            filters = parse_history_filters(params)
            payload = get_history(filters, page=page, page_size=page_size, export_scope="filtered")
            payload["summary"] = get_dashboard_summary()
            return self.json_response(payload, include_body=include_body)

        if route == "/api/prices/export.csv":
            filters = parse_history_filters(params)
            scope = params.get("scope", "filtered")
            rows = get_history_for_export(filters, scope)
            filename = "aeco-historical-prices-all.csv" if scope == "all" else "aeco-historical-prices-filtered.csv"
            return self.file_response(
                rows_to_csv(rows), "text/csv; charset=utf-8", filename, include_body=include_body
            )

        if route == "/api/prices/export.json":
            filters = parse_history_filters(params)
            scope = params.get("scope", "filtered")
            rows = get_history_for_export(filters, scope)
            filename = "aeco-historical-prices-all.json" if scope == "all" else "aeco-historical-prices-filtered.json"
            return self.file_response(
                rows_to_json(rows), "application/json; charset=utf-8", filename, include_body=include_body
            )

        if route == "/api/jobs/logs":
            limit = min(max(int(params.get("limit", "20")), 1), 100)
            return self.json_response({"items": get_recent_logs(limit=limit)}, include_body=include_body)

        if route == "/api/alerts":
            limit = min(max(int(params.get("limit", "20")), 1), 100)
            return self.json_response({"items": get_recent_alerts(limit=limit)}, include_body=include_body)

        if route == "/api/settings":
            return self.json_response(get_settings(), include_body=include_body)

        return self.json_response({"error": "Not found"}, status=HTTPStatus.NOT_FOUND, include_body=include_body)

    def do_POST(self):
        parsed, _params = parse_query(self.path)
        route = parsed.path
        body = self.read_json_body()

        if route == "/api/jobs/run":
            summary = run_fetch_job(trigger="manual")
            return self.json_response(summary)

        if route == "/api/settings":
            saved = save_settings(
                {
                    "primary_source": body.get("primary_source"),
                    "backup_source": body.get("backup_source"),
                    "run_time": body.get("run_time"),
                    "diff_threshold_percent": float(body.get("diff_threshold_percent", 3)),
                    "retention_policy": body.get("retention_policy"),
                    "enable_validation": bool(body.get("enable_validation")),
                    "enable_alert": bool(body.get("enable_alert")),
                    "enable_archive": bool(body.get("enable_archive")),
                }
            )
            return self.json_response(saved)

        return self.json_response({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

    def read_json_body(self):
        content_length = int(self.headers.get("Content-Length", 0))
        if content_length <= 0:
            return {}
        raw = self.rfile.read(content_length)
        if not raw:
            return {}
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            return {}

    def json_response(self, payload, status=HTTPStatus.OK, include_body=True):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if include_body:
            self.wfile.write(body)

    def file_response(self, payload, content_type, filename, include_body=True):
        body = payload.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if include_body:
            self.wfile.write(body)

    def serve_static(self, relative_path, include_body=True):
        safe_root = STATIC_DIR.resolve()
        target = (safe_root / relative_path).resolve()
        if not str(target).startswith(str(safe_root)) or not target.exists() or not target.is_file():
            return self.json_response(
                {"error": "Static file not found"}, status=HTTPStatus.NOT_FOUND, include_body=include_body
            )

        content_type, _encoding = mimetypes.guess_type(target.name)
        if target.suffix == ".jsx":
            content_type = "text/babel; charset=utf-8"
        elif not content_type:
            content_type = "text/plain; charset=utf-8"

        with open(target, "rb") as static_file:
            body = static_file.read()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if include_body:
            self.wfile.write(body)


def bootstrap():
    init_db()
    if not has_any_price_records():
        run_fetch_job(trigger="bootstrap")


def main():
    bootstrap()
    scheduler = DailyScheduler(run_fetch_job)
    scheduler.start()

    server = ThreadingHTTPServer((APP_HOST, APP_PORT), AecoRequestHandler)
    print(f"AECO monitor running at http://{APP_HOST}:{APP_PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        scheduler.stop()
        server.server_close()


if __name__ == "__main__":
    main()
