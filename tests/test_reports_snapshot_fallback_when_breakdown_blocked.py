from __future__ import annotations

import json
from datetime import date as date_cls
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from dotori_shopee_automation.ads.models import AdsCampaign, AdsCampaignDaily, AdsCampaignSnapshot
from dotori_shopee_automation.ads.reporting import aggregate_daily_report, render_daily_html
from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.db import EventLog, SessionLocal, init_db


def test_reports_snapshot_fallback_when_breakdown_blocked(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "report.db"
    shops_path = tmp_path / "shops.yaml"
    shops_path.write_text(
        "\n".join(
            [
                "- shop_key: samord",
                "  label: SAMORD",
                "  enabled: true",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    get_settings.cache_clear()
    init_db()

    tz_local = timezone(timedelta(hours=7))
    with SessionLocal() as session:
        session.add_all(
            [
                AdsCampaign(
                    shop_key="samord",
                    campaign_id="SHOP_TOTAL",
                    campaign_name="SHOP_TOTAL",
                ),
                AdsCampaign(
                    shop_key="samord",
                    campaign_id="cmp_a",
                    campaign_name="Campaign A",
                    status="active",
                    daily_budget=Decimal("120.00"),
                ),
                AdsCampaign(
                    shop_key="samord",
                    campaign_id="cmp_b",
                    campaign_name="Campaign B",
                    status="paused",
                    daily_budget=Decimal("80.00"),
                ),
            ]
        )
        session.add(
            AdsCampaignDaily(
                shop_key="samord",
                campaign_id="SHOP_TOTAL",
                date=date_cls(2026, 2, 16),
                spend=Decimal("77.00"),
                impressions=700,
                clicks=70,
                orders=7,
                gmv=Decimal("350.00"),
            )
        )
        session.add_all(
            [
                AdsCampaignSnapshot(
                    shop_key="samord",
                    campaign_id="cmp_a",
                    ts=datetime(2026, 2, 16, 11, 30, tzinfo=tz_local),
                    spend_today=Decimal("55.00"),
                    impressions_today=1200,
                    clicks_today=54,
                    orders_today=5,
                    gmv_today=Decimal("290.00"),
                ),
                AdsCampaignSnapshot(
                    shop_key="samord",
                    campaign_id="cmp_b",
                    ts=datetime(2026, 2, 16, 11, 35, tzinfo=tz_local),
                    spend_today=Decimal("20.00"),
                    impressions_today=800,
                    clicks_today=30,
                    orders_today=2,
                    gmv_today=Decimal("90.00"),
                ),
            ]
        )
        session.add(
            EventLog(
                level="INFO",
                message="ads_campaign_breakdown_status",
                meta_json=json.dumps(
                    {
                        "shop_key": "samord",
                        "date": "2026-02-16",
                        "blocked_403": 1,
                        "status": "cooldown_skip",
                        "reason": "cooldown_active",
                        "cooldown_until_utc": "2026-02-17T00:00:00+00:00",
                    },
                    ensure_ascii=True,
                ),
            )
        )
        session.commit()

    with SessionLocal() as session:
        data = aggregate_daily_report(session, "samord", date_cls(2026, 2, 16), as_of=None)
    html = render_daily_html(data)

    assert data["top_spend"] == []
    snapshot_fallback = data.get("snapshot_fallback") or {}
    assert int(snapshot_fallback.get("used") or 0) == 1
    assert len(snapshot_fallback.get("rows") or []) >= 1

    assert "Top chien dich theo chi tieu snapshot" in html
    assert "Campaign A" in html
    assert "Campaign B" in html
    assert ">cmp_a<" in html
    assert ">cmp_b<" in html
    assert "(no data)" not in html
