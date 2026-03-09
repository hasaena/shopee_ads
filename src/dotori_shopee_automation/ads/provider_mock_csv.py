from __future__ import annotations

import csv
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from .provider_base import Campaign, DailyMetric, SnapshotMetric
from ..config import resolve_timezone


class MockCsvProvider:
    def __init__(
        self,
        daily_csv: str | Path | None = None,
        snapshot_csv: str | Path | None = None,
        timezone: str = "Asia/Ho_Chi_Minh",
    ) -> None:
        self.daily_csv = Path(daily_csv) if daily_csv else None
        self.snapshot_csv = Path(snapshot_csv) if snapshot_csv else None
        self.timezone = timezone
        self._daily_cache: list[DailyMetric] | None = None
        self._snapshot_cache: list[SnapshotMetric] | None = None
        self._campaigns_cache: list[Campaign] | None = None

    def fetch_campaigns(self, shop_key: str) -> list[Campaign]:
        if self._campaigns_cache is not None:
            return self._campaigns_cache
        campaigns: dict[str, Campaign] = {}
        if self.daily_csv:
            for row in self._load_daily():
                campaigns[row.campaign_id] = Campaign(
                    campaign_id=row.campaign_id,
                    campaign_name=row.campaign_name,
                    status=row.status,
                    daily_budget=row.daily_budget,
                )
        if not campaigns and self.snapshot_csv:
            for row in self._load_snapshot():
                campaigns[row.campaign_id] = Campaign(
                    campaign_id=row.campaign_id,
                    campaign_name=row.campaign_name,
                    status=row.status,
                    daily_budget=row.daily_budget,
                )
        self._campaigns_cache = list(campaigns.values())
        return self._campaigns_cache

    def fetch_daily(self, shop_key: str, start: date, end: date) -> list[DailyMetric]:
        rows = self._load_daily()
        return [row for row in rows if start <= row.date <= end]

    def fetch_snapshots(
        self, shop_key: str, start: datetime, end: datetime
    ) -> list[SnapshotMetric]:
        rows = self._load_snapshot()
        return [row for row in rows if start <= row.ts <= end]

    def _load_daily(self) -> list[DailyMetric]:
        if self._daily_cache is not None:
            return self._daily_cache
        if not self.daily_csv:
            raise ValueError("daily_csv path is required")
        path = self.daily_csv
        if not path.exists():
            raise FileNotFoundError(f"Daily CSV not found: '{path}'")

        with path.open("r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            _require_columns(
                reader,
                [
                    "date",
                    "campaign_id",
                    "campaign_name",
                    "spend",
                    "impressions",
                    "clicks",
                    "orders",
                    "gmv",
                ],
            )
            rows: list[DailyMetric] = []
            for row in reader:
                rows.append(
                    DailyMetric(
                        campaign_id=_require(row, "campaign_id"),
                        campaign_name=_require(row, "campaign_name"),
                        status=_optional(row, "status"),
                        daily_budget=_optional_decimal(row, "daily_budget"),
                        date=_parse_date(_require(row, "date")),
                        spend=_parse_decimal(_require(row, "spend")),
                        impressions=_parse_int(_require(row, "impressions")),
                        clicks=_parse_int(_require(row, "clicks")),
                        orders=_parse_int(_require(row, "orders")),
                        gmv=_parse_decimal(_require(row, "gmv")),
                    )
                )
        self._daily_cache = rows
        return rows

    def _load_snapshot(self) -> list[SnapshotMetric]:
        if self._snapshot_cache is not None:
            return self._snapshot_cache
        if not self.snapshot_csv:
            raise ValueError("snapshot_csv path is required")
        path = self.snapshot_csv
        if not path.exists():
            raise FileNotFoundError(f"Snapshot CSV not found: '{path}'")

        with path.open("r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            columns = _resolve_snapshot_columns(reader)
            rows: list[SnapshotMetric] = []
            for row in reader:
                ts = _parse_datetime(_require(row, "ts"), self.timezone)
                rows.append(
                    SnapshotMetric(
                        campaign_id=_require(row, "campaign_id"),
                        campaign_name=_require(row, "campaign_name"),
                        status=_optional(row, "status"),
                        daily_budget=_optional_decimal(row, "daily_budget"),
                        ts=ts,
                        spend_today=_parse_decimal(_require(row, columns["spend"])),
                        impressions_today=_parse_int(
                            _require(row, columns["impressions"])
                        ),
                        clicks_today=_parse_int(_require(row, columns["clicks"])),
                        orders_today=_parse_int(_require(row, columns["orders"])),
                        gmv_today=_parse_decimal(_require(row, columns["gmv"])),
                    )
                )
        self._snapshot_cache = rows
        return rows


def _require_columns(reader: csv.DictReader, columns: list[str]) -> None:
    fieldnames = reader.fieldnames or []
    missing = [name for name in columns if name not in fieldnames]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")


def _resolve_snapshot_columns(reader: csv.DictReader) -> dict[str, str]:
    fieldnames = reader.fieldnames or []
    base_required = ["ts", "campaign_id", "campaign_name"]
    _require_columns(reader, base_required)

    def pick(name: str) -> str:
        if f"{name}_today" in fieldnames:
            return f"{name}_today"
        if name in fieldnames:
            return name
        raise ValueError(f"Missing required columns: {name} or {name}_today")

    return {
        "spend": pick("spend"),
        "impressions": pick("impressions"),
        "clicks": pick("clicks"),
        "orders": pick("orders"),
        "gmv": pick("gmv"),
    }


def _require(row: dict[str, str], key: str) -> str:
    value = (row.get(key) or "").strip()
    if value == "":
        raise ValueError(f"Missing value for column '{key}'")
    return value


def _optional(row: dict[str, str], key: str) -> str | None:
    value = (row.get(key) or "").strip()
    return value or None


def _optional_decimal(row: dict[str, str], key: str) -> Decimal | None:
    value = (row.get(key) or "").strip()
    if value == "":
        return None
    return _parse_decimal(value)


def _parse_decimal(value: str) -> Decimal:
    return Decimal(value)


def _parse_int(value: str) -> int:
    return int(value)


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _parse_datetime(value: str, timezone: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=resolve_timezone(timezone))
    return dt
