from __future__ import annotations

from datetime import date as date_cls
from decimal import Decimal
from pathlib import Path

from dotori_shopee_automation.ads.models import (
    AdsCampaign,
    AdsCampaignDaily,
    Phase1AdsGmsCampaignRegistry,
)
from dotori_shopee_automation.ads.reporting import aggregate_daily_report, render_daily_html
from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.db import SessionLocal, init_db


def test_report_gms_section_smoke(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "report_gms.db"
    shops_path = tmp_path / "shops.yaml"
    shops_path.write_text(
        "\n".join(
            [
                "- shop_key: minmin",
                "  label: MINMIN",
                "  enabled: true",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    get_settings.cache_clear()
    init_db()

    target_date = date_cls(2026, 3, 5)
    with SessionLocal() as session:
        session.add(
            AdsCampaign(
                shop_key="minmin",
                campaign_id="SHOP_TOTAL",
                campaign_name="SHOP_TOTAL",
            )
        )
        session.add(
            AdsCampaignDaily(
                shop_key="minmin",
                campaign_id="SHOP_TOTAL",
                date=target_date,
                spend=Decimal("220000"),
                impressions=3200,
                clicks=122,
                orders=4,
                gmv=Decimal("1540000"),
            )
        )
        session.add(
            Phase1AdsGmsCampaignRegistry(
                shop_key="minmin",
                as_of_date=target_date,
                campaign_id="gms_3001",
                campaign_type="gms",
                campaign_name="Group Campaign 3001",
                daily_budget=Decimal("100000"),
                spend=Decimal("74500"),
                source_run_dir="test",
            )
        )
        session.commit()

    with SessionLocal() as session:
        data = aggregate_daily_report(session, "minmin", target_date, as_of=None)
    assert isinstance(data.get("gms_campaigns"), list)
    assert len(data.get("gms_campaigns") or []) == 1

    html = render_daily_html(data)
    assert "Chiến dịch Group/GMS (nếu có dữ liệu)" in html
    assert "Group Campaign 3001" in html
