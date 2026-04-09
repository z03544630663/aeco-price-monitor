#!/usr/bin/env python3
"""
AECO Price Auto-Fetcher for GitHub Actions
Fetches real AECO-C prices from Gas Alberta and exports to Vercel-deployable format.
"""

import json
import hashlib
from datetime import datetime, timedelta, date
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from pathlib import Path


class SourceError(RuntimeError):
    pass


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
        raise SourceError(f"Request failed for {url}: {error}") from error
    except json.JSONDecodeError as error:
        raise SourceError(f"Invalid JSON from {url}: {error}") from error


def make_checksum(payload):
    """Create SHA256 checksum for data integrity."""
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def previous_month(target_date):
    """Get previous month's year and month."""
    pivot = target_date.replace(day=1)
    previous = pivot - timedelta(days=1)
    return previous.year, previous.month


def fetch_gas_alberta_data(reference_date=None):
    """
    Fetch AECO-C prices from Gas Alberta public source.
    Returns list of price records.
    """
    if reference_date is None:
        reference_date = date.today()
    
    fetched_at = datetime.utcnow().isoformat() + "+00:00"
    
    CURRENT_URL = "https://www.gasalberta.com/actions/charts/default?id=aeco_ng_current"
    PRIOR_URL = "https://www.gasalberta.com/actions/charts/default?id=aeco_ng_prior"
    
    records = []
    
    # Fetch current month
    try:
        current_payload = fetch_json(CURRENT_URL)
        rows = current_payload.get("data") or []
        year = reference_date.year
        month = reference_date.month
        
        for row in rows:
            day = int(row[0])
            monthly_index = row[1]
            daily_price = row[2]
            
            if daily_price is None:
                continue
            
            trade_date = date(year, month, day)
            if trade_date > reference_date:
                continue
            
            record = {
                "trade_date": trade_date.isoformat(),
                "hub": "AECO-C",
                "price_value": float(daily_price),
                "price_unit": "CAD/GJ",
                "source_key": "gas_alberta_public",
                "source_name": "Gas Alberta Public",
                "source_url": CURRENT_URL,
                "fetched_at": fetched_at,
                "status": "normal",
                "is_real_data": True,
                "data_kind": "real",
            }
            record["checksum"] = make_checksum({
                "trade_date": record["trade_date"],
                "hub": record["hub"],
                "price_value": record["price_value"],
                "price_unit": record["price_unit"],
                "source_key": record["source_key"],
                "status": record["status"],
            })
            records.append(record)
        
        print(f"✓ Fetched {len(records)} records from current month ({year}-{month:02d})")
        
    except SourceError as e:
        print(f"⚠ Current month fetch failed: {e}")
    
    # Fetch prior month
    try:
        prior_year, prior_month = previous_month(reference_date)
        prior_payload = fetch_json(PRIOR_URL)
        rows = prior_payload.get("data") or []
        
        for row in rows:
            day = int(row[0])
            monthly_index = row[1]
            daily_price = row[2]
            
            if daily_price is None:
                continue
            
            trade_date = date(prior_year, prior_month, day)
            if trade_date > reference_date:
                continue
            
            record = {
                "trade_date": trade_date.isoformat(),
                "hub": "AECO-C",
                "price_value": float(daily_price),
                "price_unit": "CAD/GJ",
                "source_key": "gas_alberta_public",
                "source_name": "Gas Alberta Public",
                "source_url": PRIOR_URL,
                "fetched_at": fetched_at,
                "status": "normal",
                "is_real_data": True,
                "data_kind": "real",
            }
            record["checksum"] = make_checksum({
                "trade_date": record["trade_date"],
                "hub": record["hub"],
                "price_value": record["price_value"],
                "price_unit": record["price_unit"],
                "source_key": record["source_key"],
                "status": record["status"],
            })
            records.append(record)
        
        print(f"✓ Fetched additional records from prior month ({prior_year}-{prior_month:02d})")
        
    except SourceError as e:
        print(f"⚠ Prior month fetch failed: {e}")
    
    return records


def export_to_vercel_format(records, output_dir):
    """Export data to Vercel-deployable JSON and HTML files."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Sort by date descending
    records = sorted(records, key=lambda r: r.get("trade_date", ""), reverse=True)
    
    if not records:
        print("⚠ No records to export!")
        return
    
    # Get latest record
    latest = records[0]
    
    # Calculate statistics
    recent_prices = [r["price_value"] for r in records[:30] if r.get("price_value")]
    avg_30d = sum(recent_prices) / len(recent_prices) if recent_prices else 0
    
    # Build summary
    summary = {
        "latest": latest,
        "retention_policy": "forever",
        "archive_total": len(records),
        "canonical_total": len([r for r in records if r.get("is_real_data")]),
        "avg_30d": round(avg_30d, 3),
        "last_job": {
            "job_name": "daily_fetch",
            "source_name": "Gas Alberta Public",
            "source_key": "gas_alberta_public",
            "run_at": datetime.utcnow().isoformat() + "+00:00",
            "result": "success",
            "message": f"Fetched {len(records)} real records."
        }
    }
    
    # Export main data file
    data = {
        "latest": latest,
        "history": records[:90],
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
    
    # Generate static HTML with embedded data
    generate_static_html(latest, summary, output_dir)
    
    print(f"✓ Exported {len(records)} records to {output_dir}")
    print(f"  Latest: {latest['trade_date']} - {latest['price_value']} {latest['price_unit']}")


def generate_static_html(latest, summary, output_dir):
    """Generate static HTML page with embedded price data."""
    trade_date = latest.get("trade_date", "Unknown")
    price = latest.get("price_value", 0)
    source_name = latest.get("source_name", "Gas Alberta Public")
    fetched_at = latest.get("fetched_at", "")
    
    # Format timestamp for display
    try:
        display_time = datetime.fromisoformat(fetched_at.replace("+00:00", "Z")).strftime("%Y-%m-%d %H:%M")
    except:
        display_time = fetched_at[:16].replace("T", " ")
    
    html_content = f'''<!doctype html>
<html lang="zh-CN">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>AECO 天然气价格监测</title>
    <style>
      * {{ margin: 0; padding: 0; box-sizing: border-box; }}
      body {{
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
        background: #0D0D0D;
        color: #fff;
        min-height: 100vh;
        display: flex;
        align-items: center;
        justify-content: center;
        padding: 20px;
      }}
      .container {{ max-width: 600px; width: 100%; }}
      .header {{ text-align: center; margin-bottom: 40px; }}
      .header h1 {{ font-size: 28px; font-weight: 600; margin-bottom: 8px; }}
      .header p {{ color: #888; font-size: 14px; }}
      .card {{
        background: #1A1A1A;
        border-radius: 12px;
        padding: 30px;
        margin-bottom: 20px;
        border: 1px solid #333;
      }}
      .card-label {{
        font-size: 13px;
        color: #888;
        margin-bottom: 8px;
        text-transform: uppercase;
        letter-spacing: 0.5px;
      }}
      .price {{
        font-size: 48px;
        font-weight: 700;
        color: #1B3A2B;
      }}
      .unit {{ font-size: 18px; color: #888; margin-left: 8px; }}
      .meta {{
        margin-top: 20px;
        padding-top: 20px;
        border-top: 1px solid #333;
        font-size: 13px;
        color: #888;
      }}
      .meta-row {{ display: flex; justify-content: space-between; margin-bottom: 8px; }}
      .status {{
        display: inline-block;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 12px;
        font-weight: 500;
      }}
      .status-normal {{ background: #1B3A2B; color: #fff; }}
      .footer {{ text-align: center; margin-top: 40px; color: #666; font-size: 12px; }}
      .update-badge {{
        background: #1B3A2B;
        color: #fff;
        padding: 6px 16px;
        border-radius: 20px;
        font-size: 12px;
        display: inline-block;
        margin-top: 10px;
      }}
      .real-data {{
        background: linear-gradient(135deg, #1B3A2B 0%, #2D5A3D 100%);
        border: 1px solid #3D7A5D;
      }}
      .stats {{
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 15px;
        margin-top: 15px;
      }}
      .stat-box {{
        background: #0D0D0D;
        padding: 15px;
        border-radius: 8px;
        text-align: center;
      }}
      .stat-value {{ font-size: 24px; font-weight: 600; color: #fff; }}
      .stat-label {{ font-size: 12px; color: #666; margin-top: 4px; }}
    </style>
  </head>
  <body>
    <div class="container">
      <div class="header">
        <h1>⚡ AECO 天然气价格监测</h1>
        <p>LinkMine · 能源驱动型计算基础设施</p>
        <span class="update-badge">数据已更新：{trade_date}</span>
      </div>

      <div class="card real-data">
        <div class="card-label">最新价格（真实数据）</div>
        <div class="price">
          {price:.3f}
          <span class="unit">CAD/GJ</span>
        </div>
        <div class="meta">
          <div class="meta-row">
            <span>交易日期</span>
            <span>{trade_date}</span>
          </div>
          <div class="meta-row">
            <span>价格中心</span>
            <span>AECO-C</span>
          </div>
          <div class="meta-row">
            <span>数据来源</span>
            <span>{source_name}</span>
          </div>
          <div class="meta-row">
            <span>状态</span>
            <span class="status status-normal">正常</span>
          </div>
          <div class="meta-row">
            <span>更新时间</span>
            <span>{display_time}</span>
          </div>
        </div>
        
        <div class="stats">
          <div class="stat-box">
            <div class="stat-value">{summary.get('archive_total', 0)}</div>
            <div class="stat-label">历史数据 (条)</div>
          </div>
          <div class="stat-box">
            <div class="stat-value">{summary.get('avg_30d', 0):.3f}</div>
            <div class="stat-label">30 日均价 (CAD/GJ)</div>
          </div>
        </div>
      </div>

      <div class="card">
        <div class="card-label">最近抓取任务</div>
        <div class="meta">
          <div class="meta-row">
            <span>任务名称</span>
            <span>daily_fetch</span>
          </div>
          <div class="meta-row">
            <span>运行时间</span>
            <span>{summary['last_job']['run_at'][:16].replace('T', ' ')}</span>
          </div>
          <div class="meta-row">
            <span>结果</span>
            <span class="status status-normal">{summary['last_job']['result']}</span>
          </div>
        </div>
      </div>

      <div class="footer">
        数据源：Gas Alberta Public · 每日自动更新
        <br/>
        <span style="color:#555">GitHub Actions + Vercel 自动部署</span>
      </div>
    </div>
  </body>
</html>'''
    
    with open(output_dir / "index.html", "w", encoding="utf-8") as f:
        f.write(html_content)
    
    print(f"✓ Generated static HTML")


def main():
    """Main entry point."""
    print("🚀 Starting AECO Price Auto-Fetch...")
    print(f"Reference date: {date.today().isoformat()}")
    
    # Fetch real data from Gas Alberta
    records = fetch_gas_alberta_data(date.today())
    
    if not records:
        print("⚠ No records fetched, exiting...")
        return
    
    # Export to Vercel format
    output_dir = Path(__file__).parent.parent / "vercel-deploy"
    export_to_vercel_format(records, output_dir)
    
    print("✅ Auto-fetch complete!")


if __name__ == "__main__":
    main()
