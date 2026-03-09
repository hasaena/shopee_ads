from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

from dotori_shopee_automation.ads.reporting import render_daily_html
from dotori_shopee_automation.ads.reporting import (
    _evaluate_metric_status,
    _kpi_threshold_reference_date,
)
from dotori_shopee_automation.scheduler import (
    _build_daily_report_discord_message,
    _render_report_markdown_summary,
)


def _sample_report_data() -> dict[str, object]:
    return {
        "shop_key": "samord",
        "shop_label": "SAMORD",
        "date": date(2026, 2, 16),
        "kind": "midday",
        "generated_at": None,
        "as_of": None,
        "data_source": "ads_daily",
        "totals": {
            "spend": Decimal("53.75"),
            "impressions": 2200,
            "clicks": 72,
            "orders": 4,
            "gmv": Decimal("290.00"),
        },
        "kpis": {
            "roas": Decimal("5.40"),
            "ctr": Decimal("0.0327"),
            "cpc": Decimal("0.75"),
            "cvr": Decimal("0.0556"),
        },
        "scorecard": {
            "budget_est": Decimal("100.00"),
            "spend": Decimal("53.75"),
            "remaining": Decimal("46.25"),
            "util_pct": Decimal("0.5375"),
            "impressions": 2200,
            "clicks": 72,
            "orders": 4,
            "gmv": Decimal("290.00"),
            "roas": Decimal("5.40"),
            "ctr": Decimal("0.0327"),
            "cpc": Decimal("0.75"),
            "cvr": Decimal("0.0556"),
        },
        "budget_source": "snapshot",
        "top_spend": [],
        "worst_roas": [],
        "delta": None,
        "campaign_breakdown_note": "Breakdown theo chien dich bi chan boi API (403 Forbidden). Chi hien thi tong shop.",
        "campaign_breakdown_status": "blocked_403",
        "snapshot_fallback": {
            "used": 1,
            "rank_key": "spend",
            "latest_snapshot_at": "2026-02-16T09:00:00+00:00",
            "rows": [
                {
                    "campaign_id": "c1",
                    "campaign_name": "Campaign One",
                    "status": "on",
                    "budget": Decimal("100.00"),
                    "spend": Decimal("35.50"),
                    "remaining": Decimal("64.50"),
                    "updated_at": "2026-02-16T09:00:00+00:00",
                }
            ],
        },
        "benchmark_7d": {
            "label": "7d",
            "days_available": 7,
            "spend_avg": Decimal("48.00"),
            "gmv_avg": Decimal("255.00"),
            "orders_avg": Decimal("3.50"),
            "roas_avg": Decimal("5.30"),
        },
        "benchmark_30d": {
            "label": "30d",
            "days_available": 30,
            "spend_avg": Decimal("45.00"),
            "gmv_avg": Decimal("240.00"),
            "orders_avg": Decimal("3.20"),
            "roas_avg": Decimal("5.10"),
        },
        "kpi_thresholds": {
            "lookback_days": 180,
            "active_days": 120,
            "min_days": 45,
            "roas": {"enabled": 1, "direction": "high", "days_available": 120, "good_cutoff": Decimal("5.00"), "watch_cutoff": Decimal("3.50")},
            "ctr": {"enabled": 1, "direction": "high", "days_available": 120, "good_cutoff": Decimal("0.0300"), "watch_cutoff": Decimal("0.0250")},
            "cvr": {"enabled": 1, "direction": "high", "days_available": 120, "good_cutoff": Decimal("0.0400"), "watch_cutoff": Decimal("0.0200")},
            "cpc": {"enabled": 1, "direction": "low", "days_available": 120, "good_cutoff": Decimal("0.80"), "watch_cutoff": Decimal("1.10")},
        },
        "kpi_evaluation": {
            "roas": {"status": "good"},
            "ctr": {"status": "good"},
            "cvr": {"status": "good"},
            "cpc": {"status": "good"},
            "gmv": {"status": "n/a"},
            "orders": {"status": "n/a"},
            "clicks": {"status": "n/a"},
            "impressions": {"status": "n/a"},
        },
        "data_sources": {
            "daily_total_source": "ads_daily",
            "campaign_breakdown_status": "blocked_403",
            "campaign_table_source": "snapshot_fallback",
            "fallback_source": "snapshot",
            "fallback_rank_key": "spend",
            "fallback_latest_snapshot_at": "2026-02-16T09:00:00+00:00",
        },
    }


def test_scorecard_surface_html_md_discord() -> None:
    data = _sample_report_data()
    html = render_daily_html(data)

    assert "Bảng chỉ số" in html
    assert "Util%" not in html
    assert "Budget Progress" not in html
    assert "Planned Daily Budget:" not in html
    assert "Đánh giá trong ngày" in html
    assert "Hiệu suất chiến dịch" in html
    assert "Today vs 7d / 30d Average" not in html
    assert "(+11.98%)" in html
    assert "(+1.89%)" in html
    assert "Huy hiệu KPI: Tốt / Ổn / Cảnh báo / Rủi ro" in html
    assert "Cửa sổ KPI: 180 ngày gần nhất" in html
    assert "Midday: chỉ áp dụng ROAS/CTR/CVR/CPC" in html
    assert "kpi-chip kpi-good" in html
    assert "kpi-chip kpi-na" not in html
    assert "kpi-chip kpi-risk" not in html
    assert "KPI chất lượng: ROAS tốt, CTR tốt, CVR tốt, CPC tốt." in html
    assert "KPI quy mô:" not in html
    assert "Trạng thái midday:" in html
    assert "Tổng kết vận hành -" in html
    assert "report-nav-open-daily" not in html
    assert "report-nav-open-weekly" not in html

    md = _render_report_markdown_summary(
        shop_key="samord",
        shop_label="SAMORD",
        report_kind="midday",
        report_date=date(2026, 2, 16),
        window_start=date(2026, 2, 16),
        window_end=date(2026, 2, 16),
        report_url=None,
        output_path=Path("D:/tmp/samord_midday.html"),
        data=data,
    )
    assert "## Bảng chỉ số" in md
    assert "| Chi tiêu | Hiển thị | Click | CTR | CPC | Đơn hàng | GMV | ROAS | CVR |" in md
    assert "Nguồn dữ liệu: daily_total_source=ads_daily" in md

    message = _build_daily_report_discord_message(
        summary="Bao cao Ads midday 2026-02-16: spend=VND 54",
        report_url="http://localhost:8000/reports/samord/daily/2026-02-16_midday.html",
        output_path=Path("D:/tmp/samord_midday.html"),
        data=data,
    )
    assert "KPI: Chi tiêu=VND 54 | GMV=VND 290 | ROAS=5.40 | Đơn hàng=4" in message
    assert "Budget:" not in message
    assert "Top snapshot:" in message


def test_kpi_four_band_and_monthly_reference() -> None:
    assert _kpi_threshold_reference_date(date(2026, 3, 18)) == date(2026, 3, 1)
    metric_cfg_high = {
        "enabled": 1,
        "direction": "high",
        "good_cutoff": Decimal("7.0"),
        "normal_cutoff": Decimal("5.0"),
        "watch_cutoff": Decimal("3.0"),
    }
    metric_cfg_low = {
        "enabled": 1,
        "direction": "low",
        "good_cutoff": Decimal("800"),
        "normal_cutoff": Decimal("1100"),
        "watch_cutoff": Decimal("1500"),
    }
    assert _evaluate_metric_status(value=Decimal("8.5"), metric_cfg=metric_cfg_high) == "good"
    assert _evaluate_metric_status(value=Decimal("6.0"), metric_cfg=metric_cfg_high) == "normal"
    assert _evaluate_metric_status(value=Decimal("4.0"), metric_cfg=metric_cfg_high) == "watch"
    assert _evaluate_metric_status(value=Decimal("2.5"), metric_cfg=metric_cfg_high) == "risk"
    assert _evaluate_metric_status(value=Decimal("700"), metric_cfg=metric_cfg_low) == "good"
    assert _evaluate_metric_status(value=Decimal("1000"), metric_cfg=metric_cfg_low) == "normal"
    assert _evaluate_metric_status(value=Decimal("1300"), metric_cfg=metric_cfg_low) == "watch"
    assert _evaluate_metric_status(value=Decimal("1900"), metric_cfg=metric_cfg_low) == "risk"
