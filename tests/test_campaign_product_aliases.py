from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

from dotori_shopee_automation.ads.alerts import ActiveAlert, alert_message
from dotori_shopee_automation.ads.campaign_labels import (
    clear_campaign_product_alias_cache,
    resolve_campaign_display_name,
)
from dotori_shopee_automation.ads.reporting import render_daily_html


def _write_aliases(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "version: 1",
                "shops:",
                "  samord:",
                "    cmp_1: Facial Serum",
                "    SHOP_TOTAL: Main Product Total",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def test_resolve_campaign_display_name_prefers_alias(monkeypatch, tmp_path: Path) -> None:
    alias_path = tmp_path / "campaign_aliases.yaml"
    _write_aliases(alias_path)
    monkeypatch.setenv("CAMPAIGN_PRODUCT_MAP_PATH", str(alias_path))
    clear_campaign_product_alias_cache()

    display = resolve_campaign_display_name(
        shop_key="samord",
        campaign_id="cmp_1",
        campaign_name="Brand Campaign A",
    )
    assert display == "Facial Serum / Brand Campaign A"

    display_only_alias = resolve_campaign_display_name(
        shop_key="samord",
        campaign_id="cmp_1",
        campaign_name="cmp_1",
    )
    assert display_only_alias == "Facial Serum"


def test_resolve_campaign_display_name_shop_total_alias(monkeypatch, tmp_path: Path) -> None:
    alias_path = tmp_path / "campaign_aliases.yaml"
    _write_aliases(alias_path)
    monkeypatch.setenv("CAMPAIGN_PRODUCT_MAP_PATH", str(alias_path))
    clear_campaign_product_alias_cache()

    display = resolve_campaign_display_name(
        shop_key="samord",
        campaign_id="SHOP_TOTAL",
        campaign_name="SHOP_TOTAL",
    )
    assert display == "Main Product Total (SHOP_TOTAL)"


def test_render_daily_html_shows_product_alias(monkeypatch, tmp_path: Path) -> None:
    alias_path = tmp_path / "campaign_aliases.yaml"
    _write_aliases(alias_path)
    monkeypatch.setenv("CAMPAIGN_PRODUCT_MAP_PATH", str(alias_path))
    clear_campaign_product_alias_cache()

    data = {
        "shop_key": "samord",
        "shop_label": "SAMORD",
        "date": date(2026, 2, 28),
        "kind": "midday",
        "generated_at": None,
        "as_of": None,
        "data_source": "ads_daily",
        "totals": {
            "spend": Decimal("33.00"),
            "impressions": 1000,
            "clicks": 30,
            "orders": 2,
            "gmv": Decimal("120.00"),
        },
        "kpis": {
            "roas": Decimal("3.63"),
            "ctr": Decimal("0.03"),
            "cpc": Decimal("1.10"),
            "cvr": Decimal("0.066"),
        },
        "scorecard": {
            "budget_est": Decimal("100.00"),
            "spend": Decimal("33.00"),
            "remaining": Decimal("67.00"),
            "util_pct": Decimal("0.33"),
            "impressions": 1000,
            "clicks": 30,
            "orders": 2,
            "gmv": Decimal("120.00"),
            "roas": Decimal("3.63"),
            "ctr": Decimal("0.03"),
            "cpc": Decimal("1.10"),
            "cvr": Decimal("0.066"),
        },
        "top_spend": [
            {
                "campaign_id": "cmp_1",
                "campaign_name": "Brand Campaign A",
                "spend": Decimal("33.00"),
                "roas": Decimal("3.63"),
            }
        ],
        "worst_roas": [],
        "campaign_performance": [
            {
                "campaign_id": "cmp_1",
                "campaign_name": "Brand Campaign A",
                "spend": Decimal("33.00"),
                "gmv": Decimal("120.00"),
                "orders": 2,
                "roas": Decimal("3.63"),
                "ctr": Decimal("0.03"),
                "cvr": Decimal("0.066"),
            }
        ],
        "campaign_performance_total": 1,
        "delta": None,
        "campaign_breakdown_note": None,
        "snapshot_fallback": {"used": 0, "rows": []},
        "data_sources": {
            "daily_total_source": "ads_daily",
            "campaign_breakdown_status": "supported",
            "campaign_table_source": "campaign_daily",
        },
    }

    html = render_daily_html(data)
    assert "Hieu suat chien dich" in html
    assert "Facial Serum / Brand Campaign A" in html
    assert "cmp_1" in html


def test_alert_message_uses_product_alias(monkeypatch, tmp_path: Path) -> None:
    alias_path = tmp_path / "campaign_aliases.yaml"
    _write_aliases(alias_path)
    monkeypatch.setenv("CAMPAIGN_PRODUCT_MAP_PATH", str(alias_path))
    clear_campaign_product_alias_cache()

    alert = ActiveAlert(
        incident_type="health_no_impressions",
        entity_type="campaign",
        entity_id="cmp_1",
        severity="WARN",
        title="No new impressions in last 60m",
        campaign_name="cmp_1",
        shop_key="samord",
        meta={"impr_delta": 0, "click_delta": 0, "impressions_today": 0},
    )
    message = alert_message(alert)
    assert "chien_dich: Facial Serum (cmp_1)" in message
