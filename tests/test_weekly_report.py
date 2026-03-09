from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from typer.testing import CliRunner

from dotori_shopee_automation.ads.incidents import AdsIncident
from dotori_shopee_automation.ads.provider_mock_csv import MockCsvProvider
from dotori_shopee_automation.ads.service import ingest_daily
from dotori_shopee_automation.ads.weekly_report import (
    build_weekly_discord_message,
    build_weekly_payload,
    compute_wow_delta,
    compute_weekly_incident_summary,
    dedupe_keep_order,
    get_last_week_range,
    week_id,
)
from dotori_shopee_automation.cli import app
from dotori_shopee_automation.config import get_settings, resolve_timezone
from dotori_shopee_automation.db import SessionLocal, init_db

runner = CliRunner()


def _write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def _setup_db(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "weekly.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    get_settings.cache_clear()
    init_db()


def test_week_range_monday() -> None:
    tz = resolve_timezone("Asia/Ho_Chi_Minh")
    now = datetime(2026, 2, 2, 9, 0, tzinfo=tz)
    start_date, end_date = get_last_week_range(now, tz)
    assert start_date == date(2026, 1, 26)
    assert end_date == date(2026, 2, 1)
    assert week_id(start_date).startswith("2026-W")


def test_weekly_report_file_and_message(tmp_path, monkeypatch) -> None:
    _setup_db(tmp_path, monkeypatch)
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

    monkeypatch.setenv("REPORTS_DIR", str(reports_dir))
    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    get_settings.cache_clear()

    csv_path = tmp_path / "daily.csv"
    _write_text(
        csv_path,
        "\n".join(
            [
                "date,campaign_id,campaign_name,status,daily_budget,spend,impressions,clicks,orders,gmv",
                "2026-01-26,cmp_1,Campaign One,ACTIVE,100.00,10.00,1000,50,5,40.00",
                "2026-01-26,cmp_2,Campaign Two,ACTIVE,100.00,20.00,1200,60,6,60.00",
                "2026-01-27,cmp_1,Campaign One,ACTIVE,100.00,10.00,1000,50,5,40.00",
                "2026-01-27,cmp_2,Campaign Two,ACTIVE,100.00,20.00,1200,60,6,60.00",
                "2026-01-28,cmp_1,Campaign One,ACTIVE,100.00,10.00,1000,50,5,40.00",
                "2026-01-28,cmp_2,Campaign Two,ACTIVE,100.00,20.00,1200,60,6,60.00",
                "2026-01-29,cmp_1,Campaign One,ACTIVE,100.00,10.00,1000,50,5,40.00",
                "2026-01-29,cmp_2,Campaign Two,ACTIVE,100.00,20.00,1200,60,6,60.00",
                "2026-01-30,cmp_1,Campaign One,ACTIVE,100.00,10.00,1000,50,5,40.00",
                "2026-01-30,cmp_2,Campaign Two,ACTIVE,100.00,20.00,1200,60,6,60.00",
                "2026-01-31,cmp_1,Campaign One,ACTIVE,100.00,10.00,1000,50,5,40.00",
                "2026-01-31,cmp_2,Campaign Two,ACTIVE,100.00,20.00,1200,60,6,60.00",
                "2026-02-01,cmp_1,Campaign One,ACTIVE,100.00,10.00,1000,50,5,40.00",
                "2026-02-01,cmp_2,Campaign Two,ACTIVE,100.00,20.00,1200,60,6,60.00",
            ]
        ),
    )

    provider = MockCsvProvider(daily_csv=csv_path)
    ingest_daily("shop_a", provider)

    result = runner.invoke(
        app,
        [
            "ads",
            "report-weekly",
            "--shop",
            "shop_a",
            "--now",
            "2026-02-02T09:00:00+07:00",
            "--no-send-discord",
        ],
    )
    assert result.exit_code == 0

    week = week_id(date(2026, 1, 26))
    report_path = reports_dir / "shop_a" / "weekly" / f"{week}.html"
    assert report_path.exists()
    html = report_path.read_text(encoding="utf-8")
    assert "Báo cáo quảng cáo theo tuần" in html
    assert week in html
    assert "Bảng chỉ số" in html
    assert "(% so với tuần trước)" in html
    assert "Huy hiệu KPI: Tốt / Ổn / Cảnh báo / Rủi ro" in html
    assert "Cửa sổ KPI: 180 ngày gần nhất (theo tuần)" in html
    assert "kpi-chip" in html
    assert "<h2>Phân bổ chi tiêu</h2>" in html
    assert "<h2>ROAS cao nhất</h2>" in html
    assert "report-nav-open-weekly" in html
    assert "report-nav-open-daily" not in html

    metrics = {
        "totals": {
            "spend": Decimal("10"),
            "orders": 1,
            "gmv": Decimal("20"),
        },
        "kpis": {"roas": Decimal("2")},
    }
    message = build_weekly_discord_message("SHOP_A", week, metrics, None, "http://test")
    assert "[SHOP_A][REPORT]" not in message
    assert week in message


def test_insights_dedup() -> None:
    items = ["a", "b", "a", "c", "b"]
    assert dedupe_keep_order(items) == ["a", "b", "c"]


def test_wow_delta_missing(tmp_path, monkeypatch) -> None:
    _setup_db(tmp_path, monkeypatch)

    csv_path = tmp_path / "daily.csv"
    _write_text(
        csv_path,
        "\n".join(
            [
                "date,campaign_id,campaign_name,status,daily_budget,spend,impressions,clicks,orders,gmv",
                "2026-01-26,cmp_1,Campaign One,ACTIVE,100.00,10.00,1000,50,5,40.00",
            ]
        ),
    )
    provider = MockCsvProvider(daily_csv=csv_path)
    ingest_daily("shop_a", provider)

    with SessionLocal() as session:
        wow = compute_wow_delta(
            session, "shop_a", date(2026, 1, 26), date(2026, 2, 1)
        )
    assert wow is None


def test_weekly_discord_message_wow_has_spaced_equals_and_signed_pct() -> None:
    metrics = {
        "totals": {"spend": Decimal("100"), "orders": 2, "gmv": Decimal("500")},
        "kpis": {"roas": Decimal("5")},
    }
    wow_delta = {
        "spend_pct": Decimal("0.277"),
        "gmv_pct": Decimal("-0.1101"),
        "roas_pct": Decimal("-0.3031"),
    }
    message = build_weekly_discord_message(
        "SHOP_A",
        "2026-W10",
        metrics,
        wow_delta,
        None,
    )
    assert "So với tuần trước: spend= +27.70% gmv= -11.01% roas= -30.31%" in message


def test_incident_summary(tmp_path, monkeypatch) -> None:
    _setup_db(tmp_path, monkeypatch)
    start_date = date(2026, 1, 26)
    end_date = date(2026, 2, 1)

    with SessionLocal() as session:
        session.add(
            AdsIncident(
                shop_key="shop_a",
                incident_type="health_no_impressions",
                entity_type="campaign",
                entity_id="cmp_1",
                status="OPEN",
                severity="WARN",
                title="test",
                message="test",
                meta_json="{}",
                first_seen_at=datetime(2026, 1, 27, 10, 0),
                last_seen_at=datetime(2026, 1, 27, 11, 0),
                last_notified_at=None,
                resolved_at=None,
            )
        )
        session.commit()

    with SessionLocal() as session:
        summary = compute_weekly_incident_summary(
            session, "shop_a", start_date, end_date
        )
    assert summary["by_type"].get("health_no_impressions") == 1


def test_top_spend_excludes_zero_spend_rows(tmp_path, monkeypatch) -> None:
    _setup_db(tmp_path, monkeypatch)
    csv_path = tmp_path / "daily_zero.csv"
    _write_text(
        csv_path,
        "\n".join(
            [
                "date,campaign_id,campaign_name,status,daily_budget,spend,impressions,clicks,orders,gmv",
                "2026-01-26,cmp_1,Campaign One,ACTIVE,100.00,0.00,1000,50,0,0.00",
                "2026-01-26,cmp_2,Campaign Two,ACTIVE,100.00,12.00,1200,60,2,45.00",
            ]
        ),
    )
    provider = MockCsvProvider(daily_csv=csv_path)
    ingest_daily("shop_a", provider)

    with SessionLocal() as session:
        payload = build_weekly_payload(session, "shop_a", date(2026, 1, 26), date(2026, 2, 1))
    assert any(row["spend"] == Decimal("0.00") for row in payload["campaign_table"])
    assert all(row["spend"] > 0 for row in payload["top_spend"])


def test_weekly_metrics_use_shop_total_and_exclude_it_from_top_lists(tmp_path, monkeypatch) -> None:
    _setup_db(tmp_path, monkeypatch)
    csv_path = tmp_path / "daily_shop_total.csv"
    _write_text(
        csv_path,
        "\n".join(
            [
                "date,campaign_id,campaign_name,status,daily_budget,spend,impressions,clicks,orders,gmv",
                "2026-01-26,SHOP_TOTAL,SHOP_TOTAL,ACTIVE,0.00,120.00,1200,120,12,600.00",
                "2026-01-26,cmp_1,Campaign One,ACTIVE,100.00,40.00,400,40,4,180.00",
                "2026-01-26,cmp_2,Campaign Two,ACTIVE,100.00,60.00,600,60,6,320.00",
            ]
        ),
    )
    provider = MockCsvProvider(daily_csv=csv_path)
    ingest_daily("shop_a", provider)

    with SessionLocal() as session:
        payload = build_weekly_payload(session, "shop_a", date(2026, 1, 26), date(2026, 2, 1))

    # scorecard totals should come from SHOP_TOTAL row only (not SHOP_TOTAL + product rows)
    assert payload["metrics"]["totals"]["spend"] == Decimal("120.00")
    # product top lists should not include SHOP_TOTAL row
    assert all(str(row["campaign_id"]).upper() != "SHOP_TOTAL" for row in payload["top_spend"])
    non_product = payload.get("non_product_pool")
    assert isinstance(non_product, dict)
    assert non_product["campaign_id"] == "NON_PRODUCT_POOL"
    assert non_product["spend"] == Decimal("20.00")


def test_top_roas_ranked_includes_non_product_by_roas_order(tmp_path, monkeypatch) -> None:
    _setup_db(tmp_path, monkeypatch)
    csv_path = tmp_path / "daily_roas_rank.csv"
    _write_text(
        csv_path,
        "\n".join(
            [
                "date,campaign_id,campaign_name,status,daily_budget,spend,impressions,clicks,orders,gmv",
                # shop total (scorecard)
                "2026-01-26,SHOP_TOTAL,SHOP_TOTAL,ACTIVE,0.00,120.00,1200,120,12,600.00",
                # product rows: roas 9.0 and 5.0
                "2026-01-26,cmp_1,Campaign One,ACTIVE,100.00,40.00,400,40,4,360.00",
                "2026-01-26,cmp_2,Campaign Two,ACTIVE,100.00,60.00,600,60,6,300.00",
            ]
        ),
    )
    provider = MockCsvProvider(daily_csv=csv_path)
    ingest_daily("shop_a", provider)

    with SessionLocal() as session:
        payload = build_weekly_payload(session, "shop_a", date(2026, 1, 26), date(2026, 2, 1))

    ranked = payload.get("top_roas_ranked") or []
    assert len(ranked) >= 3
    # NON_PRODUCT_POOL should be included but ordered by ROAS, not fixed to rank #1.
    assert any(str(row.get("campaign_id")) == "NON_PRODUCT_POOL" for row in ranked)
    assert str(ranked[0].get("campaign_id")) == "cmp_1"
