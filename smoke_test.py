import json
import socket
import subprocess
import sys
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parent
BASE_URL = "http://127.0.0.1:8000"


def wait_for_port(host, port, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(1)
            if sock.connect_ex((host, port)) == 0:
                return
        time.sleep(0.25)
    raise RuntimeError(f"server did not start on {host}:{port} within {timeout}s")


def request_json(path, method="GET", body=None):
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = Request(f"{BASE_URL}{path}", data=data, method=method, headers=headers)
    with urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def request_head(path):
    request = Request(f"{BASE_URL}{path}", method="HEAD")
    with urlopen(request, timeout=30) as response:
        return dict(response.headers.items())


def main():
    process = subprocess.Popen(
        [sys.executable, "server.py"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    try:
        wait_for_port("127.0.0.1", 8000)

        latest = request_json("/api/prices/latest")
        history = request_json("/api/prices/history?page=1&page_size=5")
        logs = request_json("/api/jobs/logs?limit=3")
        alerts = request_json("/api/alerts?limit=3")
        settings = request_json("/api/settings")
        run_result = request_json("/api/jobs/run", method="POST", body={})
        head_headers = request_head("/api/prices/export.csv?scope=all")

        summary = {
            "latest_trade_date": latest.get("item", {}).get("trade_date"),
            "latest_source": latest.get("item", {}).get("source"),
            "history_count": len(history.get("items", [])),
            "log_count": len(logs.get("items", [])),
            "alert_count": len(alerts.get("items", [])),
            "primary_source": settings.get("primary_source"),
            "manual_run_result": run_result.get("result"),
            "export_head_content_type": head_headers.get("Content-Type"),
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()


if __name__ == "__main__":
    try:
        main()
    except URLError as error:
        raise SystemExit(f"smoke test request failed: {error}") from error
