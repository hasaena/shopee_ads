from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dotori_shopee_automation.ads.alerts import load_alerts_source_totals
from dotori_shopee_automation.ads.models import AdsCampaign, AdsCampaignDaily
from dotori_shopee_automation.ads.reporting import load_report_totals_source
from dotori_shopee_automation.db import Base


def test_report_and_alerts_totals_use_same_source_of_truth(tmp_path: Path) -> None:
    db_path = tmp_path / "consistency.db"
    engine = create_engine(f"sqlite:///{db_path.as_posix()}", future=True)
    Base.metadata.create_all(bind=engine)

    with Session(engine) as session:
        session.add(
            AdsCampaign(
                shop_key="samord",
                campaign_id="SHOP_TOTAL",
                campaign_name="SHOP_TOTAL",
                status="on",
                daily_budget=Decimal("500000"),
            )
        )
        session.add(
            AdsCampaignDaily(
                shop_key="samord",
                campaign_id="SHOP_TOTAL",
                date=date(2026, 3, 5),
                spend=Decimal("310000"),
                impressions=5000,
                clicks=180,
                orders=7,
                gmv=Decimal("2580000"),
            )
        )
        session.commit()

        report_totals = load_report_totals_source(
            session,
            shop_key="samord",
            target_date=date(2026, 3, 5),
        )
        alerts_totals = load_alerts_source_totals(
            session=session,
            shop_key="samord",
            target_date=date(2026, 3, 5),
        )

    for key in ("spend", "impressions", "clicks", "orders", "gmv", "roas", "ctr", "cpc", "cvr"):
        assert report_totals.get(key) == alerts_totals.get(key)

