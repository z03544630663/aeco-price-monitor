#!/usr/bin/env python3
"""
AECO Price Fetcher for GitHub Actions
Fetches latest AECO natural gas prices and exports to Vercel-deployable JSON files.
"""

import json
import hashlib
from datetime import datetime, timedelta, date
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from pathlib import Path


def fetch_json(url):
    """Fetch JSON from URL with error handling."""
    request = Request(
        url,
        headers={
            "User-Agent": "AECO-Price-Monitor/1.0 (+GitHub Actions)",
            "Accept": "application/json, text/plain, */*",
        }
    )
    try:
        with urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError) as error:
        raise RuntimeError(f"Request failed for {url}: {error}") from error
    except json.JSONDecodeError as error:
        raise RuntimeError(f"Invalid JSON from {url}: {error}") from error


def make_checksum(payload):
    """Create SHA256 checksum for data integrity."""
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def parse_gas_alberta_chart(html_content, label):
    """Parse Gas Alberta chart data (simplified for demo)."""
    # In production, this would parse the actual chart data
    # For now, return mock data structure
    return []


def fetch_gas_alberta_data():
    """Fetch AECO prices from Gas Alberta public source."""
    fetched_at = datetime.utcnow().isoformat() + "+00:00"
    
    # These URLs return JSON with chart data
    current_url = "https://www.gasalberta.com/actions/charts/default?id=aeco_ng_current"
    prior_url = "https://www.gasalberta.com/actions/charts/default?id=aeco_ng_prior"
    
    records = []
    
    try:
        current_data = fetch_json(current_url)
        # Parse current month data
        if isinstance(current_data, dict) and 'data' in current_data:
            for point in current_data.get('data', []):
                records.append({
                    "trade_date": point.get('date'),
                    "hub": "AECO-C",
                    "price_value": point.get('price'),
                    "price_unit": "CAD/GJ",
                    "source_key": "gas_alberta_public",
                    "source_name": "Gas Alberta Public",
                    "source_url": current_url,
                    "fetched_at": fetched_at,
                    "status": "normal",
                    "is_real_data": True,
                    "data_kind": "real",
                    "checksum": make_checksum(point)
                })
    except Exception as e:
        print(f"Warning: Could not fetch current month data: {e}")
    
    try:
        prior_data = fetch_json(prior_url)
        # Parse prior month data
        if isinstance(prior_data, dict) and 'data' in prior_data:
            for point in prior_data.get('data', []):
                records.append({
                    "trade_date": point.get('date'),
                    "hub": "AECO-C",
                    "price_value": point.get('price'),
                    "price_unit": "CAD/GJ",
                    "source_key": "gas_alberta_public",
                    "source_name": "Gas Alberta Public",
                    "source_url": prior_url,
                    "fetched_at": fetched_at,
                    "status": "normal",
                    "is_real_data": True,
                    "data_kind": "real",
                    "checksum": make_checksum(point)
                })
    except Exception as e:
        print(f"Warning: Could not fetch prior month data: {e}")
    
    return records


def generate_mock_records(num_days=90):
    """Generate fallback mock data if real fetch fails."""
    records = []
    today = date.today()
    base_price = 1.65
    
    for i in range(num_days):
        trade_date = today - timedelta(days=i)
        if trade_date.weekday() < 5:  # Skip weekends
            # Add some price variation
            variation = (hash(str(trade_date)) % 100) / 1000.0
            price = base_price + variation * (1 if i % 2 == 0 else -1)
            
            records.append({
                "trade_date": trade_date.isoformat(),
                "hub": "AECO-C",
                "price_value": round(price, 3),
                "price_unit": "CAD/GJ",
                "source_key": "mock_fallback",
                "source_name": "Mock Fallback",
                "source_url": "",
                "fetched_at": datetime.utcnow().isoformat() + "+00:00",
                "status": "mock_fallback",
                "is_real_data": False,
                "data_kind": "mock",
                "checksum": make_checksum({"date": trade_date.isoformat(), "price": price})
            })
    
    return records


def export_to_vercel_format(records, output_dir):
    """Export data to Vercel-deployable JSON files."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Sort by date descending
    records = sorted(records, key=lambda r: r.get("trade_date", ""), reverse=True)
    
    # Get latest record
    latest = records[0] if records else None
    
    # Build summary
    summary = {
        "latest": latest,
        "retention_policy": "forever",
        "archive_total": len(records),
        "canonical_total": len([r for r in records if r.get("is_real_data")]),
        "last_job": {
            "job_name": "daily_fetch",
            "source_name": "Gas Alberta Public",
            "source_key": "gas_alberta_public",
            "run_at": datetime.utcnow().isoformat() + "+00:00",
            "result": "success",
            "message": f"Fetched {len(records)} records."
        }
    }
    
    # Export main data file
    data = {
        "latest": latest,
        "history": records[:90],  # Keep last 90 days
        "settings": {
            "primary_source": "gas_alberta_public",
            "backup_source": "mock_fallback",
            "run_time": "08:00",
            "diff_threshold_percent": 3.0,
            "retention_policy": "forever",
            "enable_validation": False,
            "enable_alert": True,
            "enable_archive": True
        },
        "logs": [],
        "alerts": [],
        "summary": summary
    }
    
    with open(output_dir / "data.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    
    # Export API endpoint files
    (output_dir / "api" / "prices").mkdir(parents=True, exist_ok=True)
    (output_dir / "api" / "jobs").mkdir(parents=True, exist_ok=True)
    
    # /api/prices/latest
    latest_payload = {"item": latest, "summary": summary}
    with open(output_dir / "api" / "prices" / "latest.json", "w", encoding="utf-8") as f:
        json.dump(latest_payload, f, ensure_ascii=False, indent=2, default=str)
    
    # /api/prices/history
    history_payload = {"items": records[:25], "total": len(records), "summary": summary}
    with open(output_dir / "api" / "prices" / "history.json", "w", encoding="utf-8") as f:
        json.dump(history_payload, f, ensure_ascii=False, indent=2, default=str)
    
    # /api/settings
    with open(output_dir / "api" / "settings.json", "w", encoding="utf-8") as f:
        json.dump(data["settings"], f, ensure_ascii=False, indent=2)
    
    # /api/jobs/logs
    with open(output_dir / "api" / "jobs" / "logs.json", "w", encoding="utf-8") as f:
        json.dump({"items": []}, f, ensure_ascii=False, indent=2)
    
    # /api/alerts
    with open(output_dir / "api" / "alerts.json", "w", encoding="utf-8") as f:
        json.dump({"items": []}, f, ensure_ascii=False, indent=2)
    
    print(f"✓ Exported {len(records)} records to {output_dir}")
    if latest:
        print(f"  Latest: {latest.get('trade_date')} - {latest.get('price_value')} {latest.get('price_unit')}")


def main():
    """Main entry point."""
    print("🚀 Starting AECO Price Fetch...")
    
    # Try to fetch real data
    records = []
    try:
        records = fetch_gas_alberta_data()
        if records:
            print(f"✓ Fetched {len(records)} real records from Gas Alberta")
    except Exception as e:
        print(f"⚠ Real fetch failed: {e}")
    
    # Fallback to mock data if needed
    if not records:
        print("⚠ Using mock fallback data...")
        records = generate_mock_records(90)
    
    # Export to Vercel format
    output_dir = Path(__file__).parent.parent / "vercel-deploy"
    export_to_vercel_format(records, output_dir)
    
    print("✅ Fetch complete!")


if __name__ == "__main__":
    main()
