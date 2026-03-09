from __future__ import annotations

import json
from datetime import date as date_cls
from decimal import Decimal

from dotori_shopee_automation.ads.models import AdsCampaign, AdsCampaignDaily
from dotori_shopee_automation.ads.reporting import aggregate_daily_report, render_daily_html
from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.db import EventLog, SessionLocal, init_db


def test_render_daily_html_shows_403_campaign_breakdown_message(tmp_path, monkeypatch) -> None:
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

    with SessionLocal() as session:
        session.add(
            AdsCampaign(
                shop_key="samord",
                campaign_id="SHOP_TOTAL",
                campaign_name="SHOP_TOTAL",
            )
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
        session.add(
            EventLog(
                level="INFO",
                message="ads_campaign_breakdown_status",
                meta_json=json.dumps(
                    {
                        "shop_key": "samord",
                        "date": "2026-02-16",
                        "blocked_403": 1,
                        "status": "skipped",
                        "reason": "campaign_id_list_forbidden",
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
    assert (
        data.get("campaign_breakdown_note")
        == "Breakdown theo chiến dịch bị chặn bởi API (403 Forbidden). Chỉ hiển thị tổng shop."
    )
    assert "Tổng hợp Group/Shop/Auto" in html
