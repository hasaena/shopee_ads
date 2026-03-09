from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Protocol


@dataclass(frozen=True)
class Campaign:
    campaign_id: str
    campaign_name: str
    status: str | None = None
    daily_budget: Decimal | None = None


@dataclass(frozen=True)
class DailyMetric:
    campaign_id: str
    campaign_name: str
    status: str | None
    daily_budget: Decimal | None
    date: date
    spend: Decimal
    impressions: int
    clicks: int
    orders: int
    gmv: Decimal


@dataclass(frozen=True)
class SnapshotMetric:
    campaign_id: str
    campaign_name: str
    status: str | None
    daily_budget: Decimal | None
    ts: datetime
    spend_today: Decimal
    impressions_today: int
    clicks_today: int
    orders_today: int
    gmv_today: Decimal


class AdsProvider(Protocol):
    def fetch_campaigns(self, shop_key: str) -> list[Campaign]:
        ...

    def fetch_daily(self, shop_key: str, start: date, end: date) -> list[DailyMetric]:
        ...

    def fetch_snapshots(
        self, shop_key: str, start: datetime, end: datetime
    ) -> list[SnapshotMetric]:
        ...
