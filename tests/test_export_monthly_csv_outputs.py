from __future__ import annotations

from datetime import date as date_cls
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from typer.testing import CliRunner

from dotori_shopee_automation.ads.models import AdsCampaign, AdsCampaignDaily, AdsCampaignSnapshot
from dotori_shopee_automation.cli import app
from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.db import SessionLocal, init_db


runner = CliRunner()


def test_export_monthly_csv_outputs(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "phase1.db"
    out_dir = tmp_path / "exports"
    shops_path = tmp_path / "shops.yaml"
    shops_path.write_text(
        "\n".join(
            [
                "- shop_key: samord",
                "  label: SAMORD",
                "  enabled: true",
                "  shopee_shop_id: 497412318",
                "- shop_key: minmin",
                "  label: MINMIN",
                "  enabled: true",
                "  shopee_shop_id: 567655304",
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
                    campaign_id="cmp_a",
                    campaign_name="Campaign A",
                    status="active",
                    daily_budget=Decimal("100.00"),
                ),
                AdsCampaign(
                    shop_key="minmin",
                    campaign_id="cmp_b",
                    campaign_name="Campaign B",
                    status="paused",
                    daily_budget=Decimal("80.00"),
                ),
            ]
        )
        session.add_all(
            [
                AdsCampaignDaily(
                    shop_key="samord",
                    campaign_id="cmp_a",
                    date=date_cls(2026, 2, 16),
                    spend=Decimal("53.75"),
                    impressions=2200,
                    clicks=72,
                    orders=4,
                    gmv=Decimal("290.00"),
                ),
                AdsCampaignDaily(
                    shop_key="minmin",
                    campaign_id="cmp_b",
                    date=date_cls(2026, 2, 16),
                    spend=Decimal("10.00"),
                    impressions=500,
                    clicks=20,
                    orders=1,
                    gmv=Decimal("40.00"),
                ),
            ]
        )
        session.add_all(
            [
                AdsCampaignSnapshot(
                    shop_key="samord",
                    campaign_id="cmp_a",
                    ts=datetime(2026, 2, 16, 9, 0, tzinfo=tz_local),
                    spend_today=Decimal("35.50"),
                    impressions_today=1300,
                    clicks_today=43,
                    orders_today=3,
                    gmv_today=Decimal("210.00"),
                ),
                AdsCampaignSnapshot(
                    shop_key="samord",
                    campaign_id="cmp_a",
                    ts=datetime(2026, 2, 16, 12, 0, tzinfo=tz_local),
                    spend_today=Decimal("53.75"),
                    impressions_today=2200,
                    clicks_today=72,
                    orders_today=4,
                    gmv_today=Decimal("290.00"),
                ),
                AdsCampaignSnapshot(
                    shop_key="minmin",
                    campaign_id="cmp_b",
                    ts=datetime(2026, 2, 16, 10, 0, tzinfo=tz_local),
                    spend_today=Decimal("10.00"),
                    impressions_today=500,
                    clicks_today=20,
                    orders_today=1,
                    gmv_today=Decimal("40.00"),
                ),
            ]
        )
        session.commit()

    result = runner.invoke(
        app,
        [
            "ops",
            "phase1",
            "export",
            "monthly",
            "--month",
            "2026-02",
            "--db",
            str(db_path),
            "--shops",
            "samord,minmin",
            "--out-dir",
            str(out_dir),
        ],
    )
    assert result.exit_code == 0, result.stdout
    assert "monthly_export_ok=1" in result.stdout

    shop_daily = out_dir / "shop_daily_2026-02.csv"
    shop_daily_sparse = out_dir / "shop_daily_sparse_2026-02.csv"
    shop_monthly_summary = out_dir / "shop_monthly_summary_2026-02.csv"
    snapshot_daily = out_dir / "campaign_snapshot_daily_latest_2026-02.csv"
    lifecycle = out_dir / "campaign_lifecycle_2026-02.csv"
    assert shop_daily.exists()
    assert shop_daily_sparse.exists()
    assert shop_monthly_summary.exists()
    assert snapshot_daily.exists()
    assert lifecycle.exists()

    shop_daily_text = shop_daily.read_text(encoding="utf-8")
    assert "date,shop_key,shop_label,spend,impr,clicks,ctr,cpc,orders,gmv,roas,budget_source,budget_est,remaining_est,util_pct,cvr" in shop_daily_text
    assert "2026-02-16,samord,SAMORD" in shop_daily_text

    sparse_text = shop_daily_sparse.read_text(encoding="utf-8")
    assert sparse_text.count("\n") == 3
    assert "2026-02-16,samord,SAMORD,53.75,2200,72" in sparse_text
    assert "2026-02-16,minmin,MINMIN,10.00,500,20" in sparse_text

    summary_text = shop_monthly_summary.read_text(encoding="utf-8")
    assert "month,shop_key,shop_label,total_spend,total_gmv,total_orders,roas,avg_cpc,avg_ctr,avg_cvr" in summary_text
    assert "2026-02,samord,SAMORD,53.75,290.00,4,5.40,0.75,3.27,5.56" in summary_text
    assert "2026-02,minmin,MINMIN,10.00,40.00,1,4.00,0.50,4.00,5.00" in summary_text

    snapshot_daily_text = snapshot_daily.read_text(encoding="utf-8")
    assert "day,captured_at,shop_key,shop_label,campaign_id,campaign_name,status,budget,spend,remaining,currency" in snapshot_daily_text
    assert "2026-02-16" in snapshot_daily_text
    assert "cmp_a" in snapshot_daily_text

    lifecycle_text = lifecycle.read_text(encoding="utf-8")
    assert "shop_key,shop_label,campaign_id,campaign_name,first_seen_at,last_seen_at,last_status,last_budget,last_spend,last_remaining" in lifecycle_text
    assert "samord,SAMORD,cmp_a" in lifecycle_text
