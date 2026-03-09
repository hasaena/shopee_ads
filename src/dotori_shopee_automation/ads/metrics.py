from __future__ import annotations

from decimal import Decimal
from typing import Any


def to_decimal(value: Decimal | int | float | str | None) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def nullable_decimal(value: Decimal | int | float | str | None) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return to_decimal(value)


def safe_div(numerator: Decimal, denominator: Decimal) -> Decimal | None:
    if denominator == 0:
        return None
    return numerator / denominator


def empty_totals() -> dict[str, Any]:
    return {
        "spend": Decimal("0"),
        "impressions": 0,
        "clicks": 0,
        "orders": 0,
        "gmv": Decimal("0"),
    }


def aggregate_metric_rows(
    rows: list[dict[str, Any]],
    *,
    spend_key: str = "spend",
    impressions_key: str = "impressions",
    clicks_key: str = "clicks",
    orders_key: str = "orders",
    gmv_key: str = "gmv",
) -> dict[str, Any]:
    totals = empty_totals()
    for row in rows:
        totals["spend"] += to_decimal(row.get(spend_key))
        totals["impressions"] += int(row.get(impressions_key) or 0)
        totals["clicks"] += int(row.get(clicks_key) or 0)
        totals["orders"] += int(row.get(orders_key) or 0)
        totals["gmv"] += to_decimal(row.get(gmv_key))
    return totals


def compute_kpis_from_totals(totals: dict[str, Any]) -> dict[str, Any]:
    spend = to_decimal(totals.get("spend"))
    impressions = int(totals.get("impressions") or 0)
    clicks = int(totals.get("clicks") or 0)
    orders = int(totals.get("orders") or 0)
    gmv = to_decimal(totals.get("gmv"))
    return {
        "roas": safe_div(gmv, spend),
        "ctr": safe_div(Decimal(clicks), Decimal(impressions)) if impressions > 0 else None,
        "cpc": safe_div(spend, Decimal(clicks)) if clicks > 0 else None,
        "cvr": safe_div(Decimal(orders), Decimal(clicks)) if clicks > 0 else None,
    }


def build_surface_metrics_snapshot(
    *,
    totals: dict[str, Any] | None,
    kpis: dict[str, Any] | None = None,
) -> dict[str, Any]:
    src_totals = totals if isinstance(totals, dict) else {}
    normalized_totals = {
        "spend": to_decimal(src_totals.get("spend")),
        "impressions": int(src_totals.get("impressions") or 0),
        "clicks": int(src_totals.get("clicks") or 0),
        "orders": int(src_totals.get("orders") or 0),
        "gmv": to_decimal(src_totals.get("gmv")),
    }
    normalized_kpis = (
        dict(kpis)
        if isinstance(kpis, dict)
        else compute_kpis_from_totals(normalized_totals)
    )
    # Always recompute from totals for consistency across HTML/MD/Discord surfaces.
    normalized_kpis = compute_kpis_from_totals(normalized_totals)
    return {
        "spend": normalized_totals["spend"],
        "impressions": normalized_totals["impressions"],
        "clicks": normalized_totals["clicks"],
        "orders": normalized_totals["orders"],
        "gmv": normalized_totals["gmv"],
        "roas": normalized_kpis["roas"],
        "ctr": normalized_kpis["ctr"],
        "cpc": normalized_kpis["cpc"],
        "cvr": normalized_kpis["cvr"],
    }

