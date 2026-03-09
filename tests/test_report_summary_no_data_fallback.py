from __future__ import annotations

from datetime import date
from decimal import Decimal

from dotori_shopee_automation.ads.reporting import build_discord_summary


def test_report_summary_no_data_fallback_text() -> None:
    data = {
        "date": date(2026, 2, 25),
        "kind": "midday",
        "row_count": 0,
        "totals": {
            "spend": Decimal("0"),
            "impressions": 0,
            "clicks": 0,
            "orders": 0,
            "gmv": Decimal("0"),
        },
        "kpis": {"roas": None, "ctr": None},
    }

    summary = build_discord_summary(data, report_url=None)

    assert "Bao cao Ads midday 2026-02-25" in summary
    assert "no_data=1 rows=0" in summary


def test_report_summary_includes_core_metrics_when_rows_exist() -> None:
    data = {
        "date": date(2026, 2, 25),
        "kind": "final",
        "row_count": 3,
        "totals": {
            "spend": Decimal("12345.67"),
            "impressions": 987654,
            "clicks": 4321,
            "orders": 210,
            "gmv": Decimal("67890.12"),
        },
        "kpis": {"roas": Decimal("5.50"), "ctr": Decimal("0.0123")},
    }

    summary = build_discord_summary(
        data,
        report_url="https://reports.example.com/reports/samord/daily/2026-02-25_final.html",
    )

    assert "no_data=1" not in summary
    assert "spend=VND 12,346" in summary
    assert "impressions=987,654" in summary
    assert "clicks=4,321" in summary
    assert "orders=210" in summary
    assert "gmv=VND 67,890" in summary
    assert "ROAS=5.50" in summary
    assert "CTR=1.23%" in summary
