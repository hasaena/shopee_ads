from __future__ import annotations

from datetime import date as date_cls
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from dotori_shopee_automation.ads.models import AdsCampaign, AdsCampaignDaily, AdsCampaignSnapshot
from dotori_shopee_automation.ads.reporting import aggregate_daily_report
from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.db import SessionLocal, init_db


def _write_shops(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "- shop_key: shop_a",
                "  label: SHOP_A",
                "  enabled: true",
                "  daily_budget_est: 300000",
            ]
        ),
        encoding="utf-8",
    )


def test_aggregate_daily_report_uses_shop_budget_override_when_snapshot_budget_missing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "report.db"
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)

    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    get_settings.cache_clear()
    init_db()

    with SessionLocal() as session:
        session.add(
            AdsCampaign(
                shop_key="shop_a",
                campaign_id="SHOP_TOTAL",
                campaign_name="SHOP_TOTAL",
                status="active",
                daily_budget=None,
            )
        )
        session.add(
            AdsCampaignDaily(
                shop_key="shop_a",
                campaign_id="SHOP_TOTAL",
                date=date_cls(2026, 2, 16),
                spend=Decimal("120000.00"),
                impressions=1000,
                clicks=100,
                orders=10,
                gmv=Decimal("600000.00"),
            )
        )
        session.commit()

        report = aggregate_daily_report(session, "shop_a", date_cls(2026, 2, 16), as_of=None)

    assert report["budget_source"] == "override"
    assert report["budget_est"] == Decimal("300000")
    assert report["campaigns_budgeted"] == 0
    assert report["scorecard"]["remaining"] == Decimal("180000")
    assert report["scorecard"]["util_pct"] == Decimal("0.4")


def test_aggregate_daily_report_prefers_snapshot_budget_over_override_and_skips_shop_total(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "report.db"
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)

    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    get_settings.cache_clear()
    init_db()

    tz_local = timezone(timedelta(hours=7))
    with SessionLocal() as session:
        session.add_all(
            [
                AdsCampaign(
                    shop_key="shop_a",
                    campaign_id="cmp_a",
                    campaign_name="Campaign A",
                    status="active",
                    daily_budget=Decimal("100000.00"),
                ),
                AdsCampaign(
                    shop_key="shop_a",
                    campaign_id="SHOP_TOTAL",
                    campaign_name="SHOP_TOTAL",
                    status="active",
                    daily_budget=Decimal("999999.00"),
                ),
            ]
        )
        session.add(
            AdsCampaignDaily(
                shop_key="shop_a",
                campaign_id="cmp_a",
                date=date_cls(2026, 2, 16),
                spend=Decimal("40000.00"),
                impressions=500,
                clicks=50,
                orders=2,
                gmv=Decimal("220000.00"),
            )
        )
        session.add_all(
            [
                AdsCampaignSnapshot(
                    shop_key="shop_a",
                    campaign_id="cmp_a",
                    ts=datetime(2026, 2, 16, 11, 0, tzinfo=tz_local),
                    spend_today=Decimal("40000.00"),
                    impressions_today=500,
                    clicks_today=50,
                    orders_today=2,
                    gmv_today=Decimal("220000.00"),
                ),
                AdsCampaignSnapshot(
                    shop_key="shop_a",
                    campaign_id="SHOP_TOTAL",
                    ts=datetime(2026, 2, 16, 11, 0, tzinfo=tz_local),
                    spend_today=Decimal("40000.00"),
                    impressions_today=500,
                    clicks_today=50,
                    orders_today=2,
                    gmv_today=Decimal("220000.00"),
                ),
            ]
        )
        session.commit()

        report = aggregate_daily_report(session, "shop_a", date_cls(2026, 2, 16), as_of=None)

    assert report["budget_source"] == "campaign_sum"
    assert report["budget_est"] == Decimal("100000.00")
    assert report["campaigns_budgeted"] == 1
