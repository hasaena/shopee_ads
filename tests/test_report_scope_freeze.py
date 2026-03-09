from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from dotori_shopee_automation.ads.reporting import (
    BREAKDOWN_SCOPE_NOTE,
    report_scope_line,
    render_daily_html,
)
from dotori_shopee_automation.webapp import build_phase1_status_payload


def _sample_data() -> dict[str, object]:
    return {
        "shop_key": "minmin",
        "shop_label": "MINMIN",
        "date": date(2026, 3, 5),
        "kind": "final",
        "generated_at": datetime(2026, 3, 6, 0, 0, 0),
        "as_of": None,
        "totals": {
            "spend": Decimal("120000"),
            "impressions": 1100,
            "clicks": 90,
            "orders": 3,
            "gmv": Decimal("930000"),
        },
        "kpis": {
            "roas": Decimal("7.75"),
            "ctr": Decimal("0.081818"),
            "cpc": Decimal("1333.33"),
            "cvr": Decimal("0.033333"),
        },
        "scorecard": {
            "spend": Decimal("120000"),
            "impressions": 1100,
            "clicks": 90,
            "orders": 3,
            "gmv": Decimal("930000"),
            "roas": Decimal("7.75"),
            "ctr": Decimal("0.081818"),
            "cpc": Decimal("1333.33"),
            "cvr": Decimal("0.033333"),
        },
        "campaign_performance": [],
        "campaign_performance_total": 0,
        "campaign_breakdown_note": "test",
        "snapshot_fallback": {"used": 0, "rows": []},
        "data_sources": {
            "daily_total_source": "ads_daily",
            "campaign_breakdown_status": "supported",
            "campaign_table_source": "campaign_daily",
            "breakdown_scope": "product_level_only",
            "gms_group_scope": "aggregate_only",
        },
        "breakdown_scope": "product_level_only",
        "gms_group_scope": "aggregate_only",
        "breakdown_scope_note": BREAKDOWN_SCOPE_NOTE,
    }


def test_scope_freeze_line_visible_in_html_and_status_payload(tmp_path: Path) -> None:
    data = _sample_data()
    html = render_daily_html(data)
    assert BREAKDOWN_SCOPE_NOTE in html
    assert "dotori-report-metrics" in html
    assert report_scope_line(data) == BREAKDOWN_SCOPE_NOTE

    status = build_phase1_status_payload()
    caps = status.get("capabilities") if isinstance(status, dict) else {}
    assert isinstance(caps, dict)
    assert caps.get("breakdown_scope") == "product_level_only"
    assert caps.get("gms_group_scope") == "aggregate_only"
