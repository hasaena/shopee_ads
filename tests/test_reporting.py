from datetime import date as date_cls
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from typer.testing import CliRunner

from dotori_shopee_automation.ads.models import AdsCampaign, AdsCampaignDaily, AdsCampaignSnapshot
from dotori_shopee_automation.ads.provider_mock_csv import MockCsvProvider
from dotori_shopee_automation.ads.reporting import (
    _delta_pct,
    _evaluate_scorecard_kpis,
    aggregate_daily_report,
)
from dotori_shopee_automation.ads.service import ingest_daily
from dotori_shopee_automation.cli import app
from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.db import SessionLocal, init_db

runner = CliRunner()


def _write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def test_daily_report_creates_file(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "report.db"
    reports_dir = tmp_path / "reports"
    shops_path = tmp_path / "shops.yaml"
    _write_text(
        shops_path,
        "\n".join(
            [
                "- shop_key: shop_a",
                "  label: SHOP_A",
                "  enabled: true",
            ]
        ),
    )

    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("REPORTS_DIR", str(reports_dir))
    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    get_settings.cache_clear()

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

    result = runner.invoke(
        app,
        [
            "ads",
            "report-daily",
            "--shop",
            "shop_a",
            "--kind",
            "final",
            "--date",
            "2026-02-01",
            "--no-send-discord",
        ],
    )
    assert result.exit_code == 0

    report_path = reports_dir / "shop_a" / "daily" / "2026-02-01_final.html"
    assert report_path.exists()
    html = report_path.read_text(encoding="utf-8")
    assert "Báo cáo quảng cáo hằng ngày" in html
    assert "36₫" in html


def test_daily_aggregation_prefers_campaign_rows_then_shop_total(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "report.db"
    reports_dir = tmp_path / "reports"
    shops_path = tmp_path / "shops.yaml"
    _write_text(
        shops_path,
        "\n".join(
            [
                "- shop_key: shop_a",
                "  label: SHOP_A",
                "  enabled: true",
            ]
        ),
    )

    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("REPORTS_DIR", str(reports_dir))
    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    get_settings.cache_clear()
    init_db()

    with SessionLocal() as session:
        session.add_all(
            [
                AdsCampaign(shop_key="shop_a", campaign_id="SHOP_TOTAL", campaign_name="SHOP_TOTAL"),
                AdsCampaign(shop_key="shop_a", campaign_id="1001", campaign_name="Campaign 1001"),
                AdsCampaign(shop_key="shop_a", campaign_id="1002", campaign_name="Campaign 1002"),
            ]
        )
        session.add_all(
            [
                AdsCampaignDaily(
                    shop_key="shop_a",
                    campaign_id="SHOP_TOTAL",
                    date=date_cls(2026, 2, 1),
                    spend=Decimal("100.00"),
                    impressions=1000,
                    clicks=100,
                    orders=10,
                    gmv=Decimal("500.00"),
                ),
                AdsCampaignDaily(
                    shop_key="shop_a",
                    campaign_id="1001",
                    date=date_cls(2026, 2, 1),
                    spend=Decimal("40.00"),
                    impressions=400,
                    clicks=40,
                    orders=4,
                    gmv=Decimal("200.00"),
                ),
                AdsCampaignDaily(
                    shop_key="shop_a",
                    campaign_id="1002",
                    date=date_cls(2026, 2, 1),
                    spend=Decimal("60.00"),
                    impressions=600,
                    clicks=60,
                    orders=6,
                    gmv=Decimal("300.00"),
                ),
                AdsCampaignDaily(
                    shop_key="shop_a",
                    campaign_id="SHOP_TOTAL",
                    date=date_cls(2026, 2, 2),
                    spend=Decimal("77.00"),
                    impressions=700,
                    clicks=70,
                    orders=7,
                    gmv=Decimal("350.00"),
                ),
            ]
        )
        session.commit()

        mixed = aggregate_daily_report(session, "shop_a", date_cls(2026, 2, 1), as_of=None)
        fallback = aggregate_daily_report(session, "shop_a", date_cls(2026, 2, 2), as_of=None)

    assert mixed["totals"]["spend"] == Decimal("100.00")
    assert mixed["totals"]["impressions"] == 1000
    assert mixed["totals"]["clicks"] == 100
    assert mixed["totals"]["orders"] == 10
    assert mixed["totals"]["gmv"] == Decimal("500.00")
    assert all(row["campaign_id"] != "SHOP_TOTAL" for row in mixed["top_spend"])

    assert fallback["totals"]["spend"] == Decimal("77.00")
    assert fallback["totals"]["orders"] == 7
    assert fallback["top_spend"] == []


def test_daily_totals_prefer_shop_total_when_campaign_rows_partial(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "report_partial.db"
    reports_dir = tmp_path / "reports"
    shops_path = tmp_path / "shops.yaml"
    _write_text(
        shops_path,
        "\n".join(
            [
                "- shop_key: shop_a",
                "  label: SHOP_A",
                "  enabled: true",
            ]
        ),
    )

    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("REPORTS_DIR", str(reports_dir))
    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    get_settings.cache_clear()
    init_db()

    with SessionLocal() as session:
        session.add_all(
            [
                AdsCampaign(shop_key="shop_a", campaign_id="SHOP_TOTAL", campaign_name="SHOP_TOTAL"),
                AdsCampaign(shop_key="shop_a", campaign_id="2001", campaign_name="Campaign 2001"),
                AdsCampaign(shop_key="shop_a", campaign_id="2002", campaign_name="Campaign 2002"),
            ]
        )
        session.add_all(
            [
                AdsCampaignDaily(
                    shop_key="shop_a",
                    campaign_id="SHOP_TOTAL",
                    date=date_cls(2026, 2, 3),
                    spend=Decimal("100.00"),
                    impressions=2000,
                    clicks=200,
                    orders=20,
                    gmv=Decimal("500.00"),
                ),
                # Partial campaign rows (sum spend=60) should not overwrite total spend=100.
                AdsCampaignDaily(
                    shop_key="shop_a",
                    campaign_id="2001",
                    date=date_cls(2026, 2, 3),
                    spend=Decimal("40.00"),
                    impressions=700,
                    clicks=70,
                    orders=7,
                    gmv=Decimal("180.00"),
                ),
                AdsCampaignDaily(
                    shop_key="shop_a",
                    campaign_id="2002",
                    date=date_cls(2026, 2, 3),
                    spend=Decimal("20.00"),
                    impressions=500,
                    clicks=50,
                    orders=5,
                    gmv=Decimal("90.00"),
                ),
            ]
        )
        session.commit()

        report = aggregate_daily_report(session, "shop_a", date_cls(2026, 2, 3), as_of=None)

    assert report["totals"]["spend"] == Decimal("100.00")
    assert report["totals"]["orders"] == 20
    assert len(report["top_spend"]) == 2
    coverage = report.get("campaign_spend_coverage_pct")
    assert coverage is not None
    assert coverage == Decimal("0.6")


def test_delta_pct_min_base_guard() -> None:
    # Very small previous base values should not produce misleading giant percentages.
    assert _delta_pct(
        Decimal("200000"),
        Decimal("10"),
        min_base=Decimal("10000"),
    ) is None
    assert _delta_pct(
        Decimal("200000"),
        Decimal("100000"),
        min_base=Decimal("10000"),
    ) == Decimal("1")


def test_delta_uses_previous_shop_total_when_available(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "report_delta.db"
    reports_dir = tmp_path / "reports"
    shops_path = tmp_path / "shops.yaml"
    _write_text(
        shops_path,
        "\n".join(
            [
                "- shop_key: shop_a",
                "  label: SHOP_A",
                "  enabled: true",
            ]
        ),
    )

    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("REPORTS_DIR", str(reports_dir))
    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    get_settings.cache_clear()
    init_db()

    with SessionLocal() as session:
        session.add_all(
            [
                AdsCampaign(shop_key="shop_a", campaign_id="SHOP_TOTAL", campaign_name="SHOP_TOTAL"),
                AdsCampaign(shop_key="shop_a", campaign_id="3001", campaign_name="Campaign 3001"),
            ]
        )
        session.add_all(
            [
                AdsCampaignDaily(
                    shop_key="shop_a",
                    campaign_id="SHOP_TOTAL",
                    date=date_cls(2026, 2, 28),
                    spend=Decimal("200000"),
                    impressions=2000,
                    clicks=200,
                    orders=2,
                    gmv=Decimal("900000"),
                ),
                AdsCampaignDaily(
                    shop_key="shop_a",
                    campaign_id="3001",
                    date=date_cls(2026, 2, 28),
                    spend=Decimal("100"),
                    impressions=10,
                    clicks=1,
                    orders=0,
                    gmv=Decimal("0"),
                ),
                AdsCampaignDaily(
                    shop_key="shop_a",
                    campaign_id="SHOP_TOTAL",
                    date=date_cls(2026, 3, 1),
                    spend=Decimal("100000"),
                    impressions=1000,
                    clicks=100,
                    orders=1,
                    gmv=Decimal("400000"),
                ),
            ]
        )
        session.commit()

        report = aggregate_daily_report(session, "shop_a", date_cls(2026, 3, 1), as_of=None)

    delta = report.get("delta") or {}
    assert delta.get("spend_prev") == Decimal("200000")
    assert delta.get("spend_curr") == Decimal("100000")
    assert delta.get("spend_pct") == Decimal("-0.5")


def test_midday_benchmark_uses_same_cutoff_snapshots(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "report_midday_benchmark.db"
    reports_dir = tmp_path / "reports"
    shops_path = tmp_path / "shops.yaml"
    _write_text(
        shops_path,
        "\n".join(
            [
                "- shop_key: shop_a",
                "  label: SHOP_A",
                "  enabled: true",
            ]
        ),
    )

    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("REPORTS_DIR", str(reports_dir))
    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    get_settings.cache_clear()
    init_db()

    with SessionLocal() as session:
        session.add(AdsCampaign(shop_key="shop_a", campaign_id="SHOP_TOTAL", campaign_name="SHOP_TOTAL"))
        session.add(
            AdsCampaignDaily(
                shop_key="shop_a",
                campaign_id="SHOP_TOTAL",
                date=date_cls(2026, 3, 1),
                spend=Decimal("100.00"),
                impressions=1000,
                clicks=100,
                orders=10,
                gmv=Decimal("900.00"),
            )
        )
        # Final daily totals are intentionally much larger to verify that intraday reports
        # compare against same-cutoff snapshots, not full-day totals.
        session.add_all(
            [
                AdsCampaignDaily(
                    shop_key="shop_a",
                    campaign_id="SHOP_TOTAL",
                    date=date_cls(2026, 2, 28),
                    spend=Decimal("180.00"),
                    impressions=1800,
                    clicks=180,
                    orders=18,
                    gmv=Decimal("1500.00"),
                ),
                AdsCampaignDaily(
                    shop_key="shop_a",
                    campaign_id="SHOP_TOTAL",
                    date=date_cls(2026, 2, 27),
                    spend=Decimal("160.00"),
                    impressions=1600,
                    clicks=160,
                    orders=16,
                    gmv=Decimal("1200.00"),
                ),
            ]
        )
        session.add_all(
            [
                AdsCampaignSnapshot(
                    shop_key="shop_a",
                    campaign_id="SHOP_TOTAL",
                    ts=datetime(2026, 2, 28, 12, 0, tzinfo=timezone.utc),
                    spend_today=Decimal("30.00"),
                    impressions_today=300,
                    clicks_today=30,
                    orders_today=3,
                    gmv_today=Decimal("240.00"),
                ),
                AdsCampaignSnapshot(
                    shop_key="shop_a",
                    campaign_id="SHOP_TOTAL",
                    ts=datetime(2026, 2, 28, 13, 0, tzinfo=timezone.utc),
                    spend_today=Decimal("50.00"),
                    impressions_today=500,
                    clicks_today=50,
                    orders_today=5,
                    gmv_today=Decimal("400.00"),
                ),
                AdsCampaignSnapshot(
                    shop_key="shop_a",
                    campaign_id="SHOP_TOTAL",
                    ts=datetime(2026, 2, 27, 12, 0, tzinfo=timezone.utc),
                    spend_today=Decimal("20.00"),
                    impressions_today=200,
                    clicks_today=20,
                    orders_today=2,
                    gmv_today=Decimal("160.00"),
                ),
                AdsCampaignSnapshot(
                    shop_key="shop_a",
                    campaign_id="SHOP_TOTAL",
                    ts=datetime(2026, 2, 27, 13, 0, tzinfo=timezone.utc),
                    spend_today=Decimal("40.00"),
                    impressions_today=400,
                    clicks_today=40,
                    orders_today=4,
                    gmv_today=Decimal("320.00"),
                ),
            ]
        )
        session.commit()

        report = aggregate_daily_report(
            session,
            "shop_a",
            date_cls(2026, 3, 1),
            as_of=datetime(2026, 3, 1, 13, 0, tzinfo=timezone.utc),
        )

    benchmark = report.get("benchmark") or {}
    benchmark_7d = report.get("benchmark_7d") or {}
    assert benchmark.get("basis") == "intraday_snapshot"
    assert benchmark.get("cutoff_local") == "13:00:00"
    assert benchmark_7d.get("days_available") == 2
    assert benchmark_7d.get("spend_avg") == Decimal("45.00")
    assert benchmark_7d.get("gmv_avg") == Decimal("360.00")


def test_intraday_kpi_skips_volume_metric_badges() -> None:
    scorecard = {
        "roas": Decimal("6.0"),
        "ctr": Decimal("0.04"),
        "cvr": Decimal("0.03"),
        "cpc": Decimal("700"),
        "gmv": Decimal("5000000"),
        "orders": 12,
        "clicks": 500,
        "impressions": 50000,
    }
    thresholds = {
        "roas": {"enabled": 1, "direction": "high", "good_cutoff": Decimal("5.0"), "normal_cutoff": Decimal("4.0"), "watch_cutoff": Decimal("3.0")},
        "ctr": {"enabled": 1, "direction": "high", "good_cutoff": Decimal("0.03"), "normal_cutoff": Decimal("0.02"), "watch_cutoff": Decimal("0.015")},
        "cvr": {"enabled": 1, "direction": "high", "good_cutoff": Decimal("0.02"), "normal_cutoff": Decimal("0.015"), "watch_cutoff": Decimal("0.01")},
        "cpc": {"enabled": 1, "direction": "low", "good_cutoff": Decimal("800"), "normal_cutoff": Decimal("1000"), "watch_cutoff": Decimal("1200")},
        "gmv": {"enabled": 1, "direction": "high", "good_cutoff": Decimal("1000000"), "normal_cutoff": Decimal("800000"), "watch_cutoff": Decimal("600000")},
        "orders": {"enabled": 1, "direction": "high", "good_cutoff": Decimal("10"), "normal_cutoff": Decimal("8"), "watch_cutoff": Decimal("5")},
        "clicks": {"enabled": 1, "direction": "high", "good_cutoff": Decimal("300"), "normal_cutoff": Decimal("200"), "watch_cutoff": Decimal("100")},
        "impressions": {"enabled": 1, "direction": "high", "good_cutoff": Decimal("30000"), "normal_cutoff": Decimal("20000"), "watch_cutoff": Decimal("15000")},
    }

    eval_map = _evaluate_scorecard_kpis(
        scorecard=scorecard,
        kpi_thresholds=thresholds,
        intraday=True,
    )

    assert eval_map["roas"]["status"] == "good"
    assert eval_map["ctr"]["status"] == "good"
    assert eval_map["cvr"]["status"] == "good"
    assert eval_map["cpc"]["status"] == "good"
    assert eval_map["gmv"]["status"] == "n/a"
    assert eval_map["orders"]["status"] == "n/a"
    assert eval_map["clicks"]["status"] == "n/a"
    assert eval_map["impressions"]["status"] == "n/a"
