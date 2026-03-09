from datetime import datetime
from zoneinfo import ZoneInfo

from dotori_shopee_automation.config import ShopConfig, get_settings, resolve_timezone
from dotori_shopee_automation.db import SessionLocal, init_db
from dotori_shopee_automation.ads.incidents import AdsIncident
from dotori_shopee_automation.scheduler import (
    _build_incident_digest_payload_for_shop,
    _build_daily_report_url,
    _build_weekly_report_url,
    _resolve_phase1_alerts_send_discord,
    build_scheduler,
    compute_next_daily_run,
    compute_next_weekly_run,
)


def test_compute_next_daily_run() -> None:
    tz = resolve_timezone("Asia/Ho_Chi_Minh")
    now = datetime(2026, 2, 2, 8, 0, tzinfo=tz)
    next_run = compute_next_daily_run(now, "09:00", tz)
    assert next_run.hour == 9
    assert next_run.day == 2

    now_late = datetime(2026, 2, 2, 10, 0, tzinfo=tz)
    next_run_late = compute_next_daily_run(now_late, "09:00", tz)
    assert next_run_late.day == 3
    assert next_run_late.hour == 9


def test_compute_next_weekly_run() -> None:
    tz = resolve_timezone("Asia/Ho_Chi_Minh")
    now = datetime(2026, 2, 2, 10, 0, tzinfo=tz)  # Monday
    next_run = compute_next_weekly_run(now, "MON", "09:00", tz)
    assert next_run.date().isoformat() == "2026-02-09"


def test_register_jobs_count() -> None:
    settings = get_settings()
    shops = [
        ShopConfig(shop_key="shop_a", label="SHOP_A", enabled=True),
        ShopConfig(shop_key="shop_b", label="SHOP_B", enabled=False),
    ]
    scheduler = build_scheduler(settings, shops, blocking=False)
    job_ids = {job.id for job in scheduler.get_jobs()}
    assert job_ids == {
        "ads_alerts_15m",
        "ads_daily_final_0000",
        "ads_daily_midday_1300",
        "ads_weekly_mon_0900",
    }


def test_report_url_builder_no_double_reports(monkeypatch) -> None:
    monkeypatch.setenv("REPORT_BASE_URL", "http://example.com/reports")
    monkeypatch.delenv("REPORT_ACCESS_TOKEN", raising=False)
    get_settings.cache_clear()

    daily_url = _build_daily_report_url("samord", datetime(2026, 3, 1).date(), "final", None)
    weekly_url = _build_weekly_report_url("minmin", "2026-W09", None)

    assert daily_url == "http://example.com/reports/samord/daily/2026-03-01_final.html"
    assert weekly_url == "http://example.com/reports/minmin/weekly/2026-W09.html"
    assert "/reports/reports/" not in daily_url
    assert "/reports/reports/" not in weekly_url
    get_settings.cache_clear()


def test_phase1_alerts_send_discord_override_default(monkeypatch) -> None:
    monkeypatch.delenv("PHASE1_ALERTS_SEND_DISCORD", raising=False)
    assert _resolve_phase1_alerts_send_discord(True) is True
    assert _resolve_phase1_alerts_send_discord(False) is False


def test_phase1_alerts_send_discord_override_env(monkeypatch) -> None:
    monkeypatch.setenv("PHASE1_ALERTS_SEND_DISCORD", "0")
    assert _resolve_phase1_alerts_send_discord(True) is False
    monkeypatch.setenv("PHASE1_ALERTS_SEND_DISCORD", "1")
    assert _resolve_phase1_alerts_send_discord(False) is True


def test_incident_digest_payload_counts(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "scheduler_digest.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path.as_posix()}")
    get_settings.cache_clear()
    init_db()

    tz = ZoneInfo("Asia/Ho_Chi_Minh")
    with SessionLocal() as session:
        session.add(
            AdsIncident(
                shop_key="samord",
                incident_type="health_no_impressions",
                entity_type="campaign",
                entity_id="c1",
                status="OPEN",
                severity="WARN",
                title="no impressions",
                message="msg",
                meta_json=None,
                first_seen_at=datetime(2026, 3, 1, 10, 0, tzinfo=tz),
                last_seen_at=datetime(2026, 3, 1, 11, 0, tzinfo=tz),
                last_notified_at=None,
                resolved_at=None,
            )
        )
        session.add(
            AdsIncident(
                shop_key="samord",
                incident_type="health_spend_no_orders",
                entity_type="campaign",
                entity_id="c2",
                status="RESOLVED",
                severity="CRITICAL",
                title="spend no orders",
                message="msg",
                meta_json=None,
                first_seen_at=datetime(2026, 3, 1, 8, 0, tzinfo=tz),
                last_seen_at=datetime(2026, 3, 2, 1, 0, tzinfo=tz),
                last_notified_at=None,
                resolved_at=datetime(2026, 3, 2, 1, 0, tzinfo=tz),
            )
        )
        session.commit()

    with SessionLocal() as session:
        digest = _build_incident_digest_payload_for_shop(
            session=session,
            shop_key="samord",
            report_date=datetime(2026, 3, 1).date(),
        )

    assert digest["opened"] == 2
    assert digest["resolved"] == 0
    assert digest["open_end_of_day"] >= 1
    assert digest["critical"] == 1
    assert digest["warn"] == 1
    assert digest["has_incident"] == 1
    get_settings.cache_clear()
