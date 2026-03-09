from pathlib import Path

from dotori_shopee_automation.ads.models import AdsCampaign, AdsCampaignDaily
from dotori_shopee_automation.ads.provider_mock_csv import MockCsvProvider
from dotori_shopee_automation.ads.service import ingest_daily
from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.db import SessionLocal, init_db


def _write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def _setup_db(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    get_settings.cache_clear()
    init_db()


def test_ingest_daily_idempotent(tmp_path, monkeypatch) -> None:
    _setup_db(tmp_path, monkeypatch)
    csv_path = tmp_path / "daily.csv"
    _write_text(
        csv_path,
        "\n".join(
            [
                "date,campaign_id,campaign_name,status,daily_budget,spend,impressions,clicks,orders,gmv",
                "2026-02-01,cmp_1,Campaign One,ACTIVE,100.00,25.50,1000,30,3,150.00",
                "2026-02-01,cmp_2,Campaign Two,ACTIVE,80.00,10.00,500,12,1,60.00",
            ]
        ),
    )

    provider = MockCsvProvider(daily_csv=csv_path)
    ingest_daily("shop_a", provider)

    with SessionLocal() as session:
        assert session.query(AdsCampaign).count() == 2
        assert session.query(AdsCampaignDaily).count() == 2

    ingest_daily("shop_a", provider)
    with SessionLocal() as session:
        assert session.query(AdsCampaign).count() == 2
        assert session.query(AdsCampaignDaily).count() == 2
