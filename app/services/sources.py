import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from app.db import utc_now_iso


class SourceError(RuntimeError):
    pass


def previous_month(target_date):
    pivot = target_date.replace(day=1)
    previous = pivot - timedelta(days=1)
    return previous.year, previous.month


def make_checksum(payload):
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def fetch_json(url):
    request = Request(
        url,
        headers={
            "User-Agent": "AECO-Price-Monitor/1.0 (+internal tool)",
            "Accept": "application/json, text/plain, */*",
        },
    )
    try:
        with urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError) as error:
        raise SourceError(f"request failed for {url}: {error}") from error
    except json.JSONDecodeError as error:
        raise SourceError(f"invalid json from {url}: {error}") from error


@dataclass
class FetchResult:
    records: list
    metadata: dict


class GasAlbertaPublicAdapter:
    key = "gas_alberta_public"
    name = "Gas Alberta Public"
    hub = "AECO-C"
    unit = "CAD/GJ"

    CURRENT_URL = "https://www.gasalberta.com/actions/charts/default?id=aeco_ng_current"
    PRIOR_URL = "https://www.gasalberta.com/actions/charts/default?id=aeco_ng_prior"

    def fetch_recent(self, reference_date):
        fetched_at = utc_now_iso()
        current_payload = fetch_json(self.CURRENT_URL)
        prior_payload = fetch_json(self.PRIOR_URL)

        records = []
        records.extend(
            self._build_records(
                current_payload,
                fetched_at=fetched_at,
                year=reference_date.year,
                month=reference_date.month,
                source_url=self.CURRENT_URL,
                payload_label="current_month",
                max_trade_date=reference_date,
            )
        )

        prior_year, prior_month = previous_month(reference_date)
        records.extend(
            self._build_records(
                prior_payload,
                fetched_at=fetched_at,
                year=prior_year,
                month=prior_month,
                source_url=self.PRIOR_URL,
                payload_label="prior_month",
                max_trade_date=reference_date,
            )
        )
        return FetchResult(
            records=records,
            metadata={
                "fetched_at": fetched_at,
                "record_count": len(records),
                "source_urls": [self.CURRENT_URL, self.PRIOR_URL],
            },
        )

    def _build_records(self, payload, fetched_at, year, month, source_url, payload_label, max_trade_date):
        rows = payload.get("data") or []
        built = []
        now = utc_now_iso()
        for row in rows:
            day = int(row[0])
            monthly_index = row[1]
            daily_price = row[2]
            trade_date = date(year, month, day)

            if trade_date > max_trade_date:
                continue
            if daily_price is None:
                continue

            normalized = {
                "trade_date": trade_date.isoformat(),
                "hub": self.hub,
                "price_value": float(daily_price),
                "price_unit": self.unit,
                "source_key": self.key,
                "source_name": self.name,
                "source_url": source_url,
                "fetched_at": fetched_at,
                "raw_payload": {
                    "payload_label": payload_label,
                    "row": row,
                    "monthly_index": monthly_index,
                    "daily_price": daily_price,
                },
                "status": "normal",
                "is_real_data": True,
                "created_at": now,
                "updated_at": now,
            }
            normalized["checksum"] = make_checksum(
                {
                    "trade_date": normalized["trade_date"],
                    "hub": normalized["hub"],
                    "price_value": normalized["price_value"],
                    "price_unit": normalized["price_unit"],
                    "source_key": normalized["source_key"],
                    "status": normalized["status"],
                }
            )
            built.append(normalized)
        return built


class IceNgxReservedAdapter:
    key = "ice_ngx_reserved"
    name = "ICE NGX (Reserved)"

    def fetch_recent(self, reference_date):
        raise SourceError("ICE NGX adapter is reserved for future commercial credentials.")


class DobEnergyPublicAdapter:
    key = "dob_energy_public"
    name = "DOB Energy Public"
    hub = "AECO-C"
    unit = "CAD/GJ"
    PAGE_URL = "https://www.dobenergy.com/data/markets/prices/"

    def fetch_recent(self, reference_date):
        fetched_at = utc_now_iso()
        request = Request(
            self.PAGE_URL,
            headers={
                "User-Agent": "AECO-Price-Monitor/1.0 (+internal tool)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )
        try:
            with urlopen(request, timeout=30) as response:
                html = response.read().decode("utf-8", "ignore")
        except (HTTPError, URLError, TimeoutError) as error:
            raise SourceError(f"request failed for {self.PAGE_URL}: {error}") from error

        records = self._build_records_from_html(html, fetched_at=fetched_at, max_trade_date=reference_date)
        return FetchResult(
            records=records,
            metadata={
                "fetched_at": fetched_at,
                "record_count": len(records),
                "source_urls": [self.PAGE_URL],
            },
        )

    def _build_records_from_html(self, html, fetched_at, max_trade_date):
        match = re.search(r'\{"name": "AECO/NGX Spot Price", "data": \[(.*?)\]\}', html, re.S)
        if not match:
            raise SourceError("DOB Energy page does not contain AECO/NGX Spot Price history.")

        points = re.findall(r"\[(\d+),\s*([0-9.]+)\]", match.group(1))
        if not points:
            raise SourceError("DOB Energy page returned an empty AECO/NGX Spot Price series.")

        now = utc_now_iso()
        built = []
        for timestamp_ms, price_value in points:
            trade_date = datetime.utcfromtimestamp(int(timestamp_ms) / 1000).date()
            if trade_date > max_trade_date:
                continue

            normalized = {
                "trade_date": trade_date.isoformat(),
                "hub": self.hub,
                "price_value": float(price_value),
                "price_unit": self.unit,
                "source_key": self.key,
                "source_name": self.name,
                "source_url": self.PAGE_URL,
                "fetched_at": fetched_at,
                "raw_payload": {
                    "series_name": "AECO/NGX Spot Price",
                    "timestamp_ms": int(timestamp_ms),
                    "price_value": float(price_value),
                },
                "status": "normal",
                "is_real_data": True,
                "created_at": now,
                "updated_at": now,
            }
            normalized["checksum"] = make_checksum(
                {
                    "trade_date": normalized["trade_date"],
                    "hub": normalized["hub"],
                    "price_value": normalized["price_value"],
                    "price_unit": normalized["price_unit"],
                    "source_key": normalized["source_key"],
                    "status": normalized["status"],
                }
            )
            built.append(normalized)
        return built


class MockFallbackAdapter:
    key = "mock_fallback"
    name = "Mock Fallback"
    hub = "AECO-C"
    unit = "CAD/GJ"

    def build_records(self, missing_dates, anchor_price=1.65):
        if not missing_dates:
            return []

        missing_dates = sorted(missing_dates)
        baseline = float(anchor_price or 1.65)
        now = utc_now_iso()
        start = missing_dates[0]
        records = []

        for trade_date in missing_dates:
            offset = (trade_date - start).days
            seasonal = math.sin(offset / 7.3) * 0.08
            wave = math.cos(offset / 17.0) * 0.05
            drift = (offset / max(len(missing_dates), 1)) * 0.04
            price_value = round(max(0.4, baseline - 0.18 + seasonal + wave + drift), 3)
            normalized = {
                "trade_date": trade_date.isoformat(),
                "hub": self.hub,
                "price_value": price_value,
                "price_unit": self.unit,
                "source_key": self.key,
                "source_name": self.name,
                "source_url": "",
                "fetched_at": now,
                "raw_payload": {
                    "reason": "real-history-unavailable",
                    "anchor_price": baseline,
                    "offset_days": offset,
                },
                "status": "mock_fallback",
                "is_real_data": False,
                "created_at": now,
                "updated_at": now,
            }
            normalized["checksum"] = make_checksum(
                {
                    "trade_date": normalized["trade_date"],
                    "hub": normalized["hub"],
                    "price_value": normalized["price_value"],
                    "price_unit": normalized["price_unit"],
                    "source_key": normalized["source_key"],
                    "status": normalized["status"],
                }
            )
            records.append(normalized)
        return records


def get_source_adapter(source_key):
    if source_key == "gas_alberta_public":
        return GasAlbertaPublicAdapter()
    if source_key == "ice_ngx_reserved":
        return IceNgxReservedAdapter()
    if source_key == "dob_energy_public":
        return DobEnergyPublicAdapter()
    if source_key == "mock_fallback":
        return MockFallbackAdapter()
    raise SourceError(f"unknown source adapter: {source_key}")
