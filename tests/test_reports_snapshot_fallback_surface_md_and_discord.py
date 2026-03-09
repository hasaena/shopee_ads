from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from dotori_shopee_automation.scheduler import (
    _build_daily_report_discord_message,
    _render_report_markdown_summary,
)


def test_snapshot_fallback_surface_in_md_and_discord_when_blocked() -> None:
    output_path = Path("D:/tmp/samord_midday.html")
    data = {
        "totals": {
            "spend": Decimal("15345.50"),
            "impressions": 120000,
            "clicks": 2200,
            "orders": 38,
            "gmv": Decimal("45000.00"),
        },
        "kpis": {"roas": Decimal("2.93"), "ctr": Decimal("0.0183")},
        "top_spend": [],
        "snapshot_fallback": {
            "used": 1,
            "rank_key": "spend",
            "latest_snapshot_at": datetime(2026, 2, 16, 9, 0, tzinfo=timezone.utc),
            "rows": [
                {
                    "campaign_id": "cmp_a",
                    "campaign_name": "Alpha Campaign Name For Fallback",
                    "status": "on",
                    "budget": Decimal("10000.00"),
                    "spend": Decimal("1250.50"),
                    "updated_at": "2026-02-16T09:00:00+00:00",
                },
                {
                    "campaign_id": "cmp_b",
                    "campaign_name": "Beta Campaign",
                    "status": "off",
                    "budget": Decimal("500.00"),
                    "spend": Decimal("900.00"),
                    "updated_at": "2026-02-16T09:00:00+00:00",
                },
            ],
        },
    }

    markdown = _render_report_markdown_summary(
        shop_key="samord",
        shop_label="SAMORD",
        report_kind="midday",
        report_date=date(2026, 2, 16),
        window_start=date(2026, 2, 16),
        window_end=date(2026, 2, 16),
        report_url=None,
        output_path=output_path,
        data=data,
    )
    assert "## Top campaign (snapshot fallback)" in markdown
    assert "| Chiến dịch | Trạng thái | Ngân sách | Chi tiêu | Còn lại | Cập nhật |" in markdown
    assert "Alpha Campaign Name For Fallback (cmp_a)" in markdown
    assert "| Beta Campaign (cmp_b) | off | VND 500 | VND 900 | VND 0 |" in markdown
    assert "Dữ liệu: rank_key=spend" in markdown

    message = _build_daily_report_discord_message(
        summary="Bao cao Ads midday 2026-02-16: spend=VND 15,346",
        report_url="http://localhost:8000/reports/samord/daily/2026-02-16_midday.html",
        output_path=output_path,
        data=data,
    )
    assert "Top snapshot:" in message
    assert "chi tiêu=VND 1,251" in message
    assert "còn lại=VND 0" in message
    snippet_lines = [line for line in message.splitlines() if line.startswith("Top snapshot:")]
    assert snippet_lines
    assert len(snippet_lines[0]) <= 200


def test_snapshot_fallback_not_surface_when_campaign_breakdown_supported() -> None:
    output_path = Path("D:/tmp/minmin_midday.html")
    data = {
        "totals": {
            "spend": Decimal("300.00"),
            "impressions": 1000,
            "clicks": 40,
            "orders": 5,
            "gmv": Decimal("1000.00"),
        },
        "kpis": {"roas": Decimal("3.33"), "ctr": Decimal("0.04")},
        "top_spend": [
            {
                "campaign_id": "cmp_live",
                "campaign_name": "Live Campaign",
                "spend": Decimal("300.00"),
                "roas": Decimal("3.33"),
            }
        ],
        "snapshot_fallback": {"used": 0, "rows": [], "rank_key": None, "latest_snapshot_at": None},
    }

    markdown = _render_report_markdown_summary(
        shop_key="minmin",
        shop_label="MINMIN",
        report_kind="midday",
        report_date=date(2026, 2, 16),
        window_start=date(2026, 2, 16),
        window_end=date(2026, 2, 16),
        report_url=None,
        output_path=output_path,
        data=data,
    )
    assert "## Top campaign (snapshot fallback)" not in markdown

    message = _build_daily_report_discord_message(
        summary="Bao cao Ads midday 2026-02-16: spend=VND 300",
        report_url="https://reports.example.com/reports/minmin/daily/2026-02-16_midday.html",
        output_path=output_path,
        data=data,
    )
    assert "Top snapshot:" not in message
