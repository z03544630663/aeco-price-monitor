import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
DATABASE_PATH = BASE_DIR / "data.sqlite3"
SCHEMA_PATH = BASE_DIR / "schema.sql"
STATIC_DIR = BASE_DIR / "static"

APP_HOST = os.getenv("AECO_MONITOR_HOST", "127.0.0.1")
APP_PORT = int(os.getenv("AECO_MONITOR_PORT", "8000"))
APP_TIMEZONE = os.getenv("AECO_MONITOR_TIMEZONE", "Asia/Shanghai")

DEFAULT_SETTINGS = {
    "primary_source": "gas_alberta_public",
    "backup_source": "dob_energy_public",
    "run_time": "08:00",
    "diff_threshold_percent": 3.0,
    "retention_policy": "forever",
    "enable_validation": True,
    "enable_alert": True,
    "enable_archive": True,
}

SOURCE_OPTIONS = [
    {
        "key": "gas_alberta_public",
        "name": "Gas Alberta Public",
        "kind": "real",
        "description": "公开源，提供 AECO/AB-NIT 本月与上月日度现货数据。",
        "supports_validation": True,
    },
    {
        "key": "ice_ngx_reserved",
        "name": "ICE NGX (Reserved)",
        "kind": "commercial",
        "description": "商业源预留，占位适配器，当前版本不伪造数据。",
        "supports_validation": False,
    },
    {
        "key": "dob_energy_public",
        "name": "DOB Energy Public",
        "kind": "real",
        "description": "公开商品价格页，内嵌 AECO/NGX Spot Price 历史序列，可用作备源与长历史补充。",
        "supports_validation": True,
    },
    {
        "key": "mock_fallback",
        "name": "Mock Fallback",
        "kind": "mock",
        "description": "仅在真实历史不足或抓取失败时补位，前端会明确标识为模拟数据。",
        "supports_validation": False,
    },
]

EXPORT_FIELDS = [
    "trade_date",
    "hub",
    "price",
    "unit",
    "source",
    "source_key",
    "fetched_at",
    "status",
    "data_kind",
]
