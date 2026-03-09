from __future__ import annotations

from datetime import date as date_cls
from decimal import Decimal
from pathlib import Path

from typer.testing import CliRunner

from dotori_shopee_automation.ads.models import AdsCampaign, AdsCampaignDaily
from dotori_shopee_automation.cli import app
from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.db import SessionLocal, init_db


runner = CliRunner()


def _write_shops(path: Path) -> None:
    lines = [
        "- shop_key: samord",
        "  label: SAMORD",
        "  enabled: true",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def test_daily_report_top_campaigns_from_campaign_daily(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "report.db"
    reports_dir = tmp_path / "reports"
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)

    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("REPORTS_DIR", str(reports_dir))
    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    get_settings.cache_clear()
    init_db()

    with SessionLocal() as session:
        session.add_all(
            [
                AdsCampaign(shop_key="samord", campaign_id="cmp_a", campaign_name="Campaign A"),
                AdsCampaign(shop_key="samord", campaign_id="cmp_b", campaign_name="Campaign B"),
                AdsCampaign(shop_key="samord", campaign_id="cmp_c", campaign_name="Campaign C"),
            ]
        )
        session.add_all(
            [
                AdsCampaignDaily(
                    shop_key="samord",
                    campaign_id="cmp_a",
                    date=date_cls(2026, 2, 16),
                    spend=Decimal("110.00"),
                    impressions=2000,
                    clicks=60,
                    orders=4,
                    gmv=Decimal("880.00"),
                ),
                AdsCampaignDaily(
                    shop_key="samord",
                    campaign_id="cmp_b",
                    date=date_cls(2026, 2, 16),
                    spend=Decimal("80.00"),
                    impressions=1500,
                    clicks=45,
                    orders=2,
                    gmv=Decimal("420.00"),
                ),
                AdsCampaignDaily(
                    shop_key="samord",
                    campaign_id="cmp_c",
                    date=date_cls(2026, 2, 16),
                    spend=Decimal("35.00"),
                    impressions=900,
                    clicks=22,
                    orders=1,
                    gmv=Decimal("120.00"),
                ),
            ]
        )
        session.commit()

    result = runner.invoke(
        app,
        [
            "ads",
            "report-daily",
            "--shop",
            "samord",
            "--kind",
            "final",
            "--date",
            "2026-02-16",
            "--no-send-discord",
        ],
    )
    assert result.exit_code == 0, result.stdout

    report_path = reports_dir / "samord" / "daily" / "2026-02-16_final.html"
    assert report_path.exists()
    html = report_path.read_text(encoding="utf-8")
    assert "Hiệu suất chiến dịch" in html
    assert "Campaign A" in html
    assert "Campaign B" in html
    assert "Campaign C" in html
    assert "Không có breakdown theo chiến dịch cho ngày này" not in html
