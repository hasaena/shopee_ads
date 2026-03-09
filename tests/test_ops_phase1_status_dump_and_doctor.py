from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import os
from pathlib import Path

from dotori_shopee_automation import cli
from dotori_shopee_automation.ads.models import AdsCampaign, AdsCampaignDaily, AdsCampaignSnapshot
from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.db import SessionLocal, init_db
from dotori_shopee_automation.ops.doctor_notify import OpsDoctorNotifyState
from dotori_shopee_automation.shopee.token_store import upsert_token


def _write_phase1_shops(path: Path) -> None:
    lines = [
        "- shop_key: samord",
        "  label: SAMORD",
        "  enabled: true",
        "  shopee_shop_id: 497412318",
        "- shop_key: minmin",
        "  label: MINMIN",
        "  enabled: true",
        "  shopee_shop_id: 567655304",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def _configure_test_env(monkeypatch, tmp_path: Path) -> Path:
    shops_path = tmp_path / "shops.yaml"
    reports_dir = tmp_path / "reports"
    db_path = tmp_path / "task104.db"
    _write_phase1_shops(shops_path)
    reports_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path.as_posix()}")
    monkeypatch.setenv("REPORTS_DIR", str(reports_dir))
    monkeypatch.setenv("REPORT_BASE_URL", "http://localhost:8000")
    monkeypatch.setenv("REPORT_ACCESS_TOKEN", "TEST_TOKEN_DO_NOT_USE")
    monkeypatch.setenv("TIMEZONE", "Asia/Ho_Chi_Minh")
    get_settings.cache_clear()
    return reports_dir


def _seed_tokens_and_ads(now_utc: datetime) -> None:
    init_db()
    session = SessionLocal()
    try:
        upsert_token(
            session,
            "samord",
            497412318,
            "A" * 40,
            "R" * 40,
            now_utc + timedelta(hours=3),
            now_utc + timedelta(days=30),
        )
        upsert_token(
            session,
            "minmin",
            567655304,
            "B" * 40,
            "S" * 40,
            now_utc + timedelta(hours=3),
            now_utc + timedelta(days=30),
        )
        for shop_key in ("samord", "minmin"):
            campaign_id = f"campaign_{shop_key}"
            session.add(
                AdsCampaign(
                    shop_key=shop_key,
                    campaign_id=campaign_id,
                    campaign_name=f"{shop_key} campaign",
                    status="RUNNING",
                    daily_budget=100,
                )
            )
            session.add(
                AdsCampaignDaily(
                    shop_key=shop_key,
                    campaign_id=campaign_id,
                    date=date.today(),
                    spend=10,
                    impressions=100,
                    clicks=10,
                    orders=1,
                    gmv=20,
                )
            )
            session.add(
                AdsCampaignSnapshot(
                    shop_key=shop_key,
                    campaign_id=campaign_id,
                    ts=now_utc - timedelta(minutes=5),
                    spend_today=10,
                    impressions_today=100,
                    clicks_today=10,
                    orders_today=1,
                    gmv_today=20,
                )
            )
        session.commit()
    finally:
        session.close()


def _write_report(path: Path, text: str, ts: datetime) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    epoch = int(ts.timestamp())
    os.utime(path, (epoch, epoch))


def test_status_dump_builder_contains_phase1_report_keys_and_sorted_issues(
    monkeypatch, tmp_path: Path
) -> None:
    reports_root = _configure_test_env(monkeypatch, tmp_path)
    now_utc = datetime.now(timezone.utc)
    _seed_tokens_and_ads(now_utc)

    _write_report(
        reports_root / "samord" / "daily" / "2026-02-25_midday.html",
        "<html>samord midday</html>",
        now_utc,
    )
    _write_report(
        reports_root / "minmin" / "daily" / "2026-02-25_midday.html",
        "<html>minmin midday</html>",
        now_utc,
    )
    _write_report(
        reports_root / "minmin" / "daily" / "2026-02-24_final.html",
        "<html>minmin final</html>",
        now_utc,
    )
    _write_report(
        reports_root / "minmin" / "weekly" / "2026-W08.html",
        "<html>minmin weekly</html>",
        now_utc,
    )

    payload = cli._build_phase1_status_payload_for_cli(
        shops_value="samord,minmin",
        reports_dir=str(reports_root),
    )

    assert sorted(payload.get("shops") or []) == ["minmin", "samord"]
    latest = ((payload.get("reports") or {}).get("latest") or {})
    assert sorted(latest.keys()) == ["minmin", "samord"]
    for shop_key in ("samord", "minmin"):
        assert set((latest.get(shop_key) or {}).keys()) == {
            "daily_midday",
            "daily_final",
            "weekly",
        }

    minmin_weekly = latest["minmin"]["weekly"]
    assert isinstance(minmin_weekly, dict)
    assert "token=***" in str(minmin_weekly.get("url") or "")

    doctor_notify = payload.get("doctor_notify") or {}
    assert sorted(doctor_notify.keys()) == ["minmin", "samord"]
    assert doctor_notify["samord"]["last_action"] in {"never", "ok"}
    assert doctor_notify["minmin"]["last_action"] in {"never", "ok"}

    issues = [row for row in payload.get("issues", []) if isinstance(row, dict)]
    assert any(
        row.get("shop") == "samord" and row.get("code") == "REPORT_MISSING_DAILY_FINAL"
        for row in issues
    )
    severity_order = {"error": 0, "warn": 1, "info": 2}
    assert issues == sorted(
        issues,
        key=lambda row: (
            str(row.get("shop") or ""),
            severity_order.get(str(row.get("severity") or "info"), 9),
            str(row.get("code") or ""),
        ),
    )
    get_settings.cache_clear()


def test_doctor_exit_code_mapping() -> None:
    assert cli._phase1_doctor_exit_code([]) == 0
    assert cli._phase1_doctor_exit_code([{"severity": "info"}]) == 0
    assert cli._phase1_doctor_exit_code([{"severity": "warn"}]) == 1
    assert cli._phase1_doctor_exit_code(
        [{"severity": "warn"}, {"severity": "error"}]
    ) == 2


def test_status_dump_includes_doctor_notify_state(monkeypatch, tmp_path: Path) -> None:
    reports_root = _configure_test_env(monkeypatch, tmp_path)
    now_utc = datetime.now(timezone.utc)
    _seed_tokens_and_ads(now_utc)
    init_db()
    session = SessionLocal()
    try:
        session.add(
            OpsDoctorNotifyState(
                shop_label="SAMORD",
                last_alert_at="2026-02-27T03:00:00Z",
                last_alert_level="warn",
                cooldown_until="2026-02-27T04:00:00Z",
            )
        )
        session.commit()
    finally:
        session.close()

    payload = cli._build_phase1_status_payload_for_cli(
        shops_value="samord,minmin",
        reports_dir=str(reports_root),
    )
    doctor_notify = payload.get("doctor_notify") or {}
    assert doctor_notify["samord"]["last_action"] == "alert"
    assert doctor_notify["samord"]["last_sent_at"] == "2026-02-27T03:00:00Z"
    assert doctor_notify["samord"]["cooldown_until"] == "2026-02-27T04:00:00Z"
    assert doctor_notify["minmin"]["last_action"] in {"never", "ok"}
    get_settings.cache_clear()


def test_status_dump_uses_explicit_doctor_action_and_send_timestamp(
    monkeypatch, tmp_path: Path
) -> None:
    reports_root = _configure_test_env(monkeypatch, tmp_path)
    now_utc = datetime.now(timezone.utc)
    _seed_tokens_and_ads(now_utc)
    init_db()
    session = SessionLocal()
    try:
        session.add(
            OpsDoctorNotifyState(
                shop_label="SAMORD",
                last_action="alert",
                last_sent_at=None,
                cooldown_until="2026-02-27T04:00:00Z",
            )
        )
        session.add(
            OpsDoctorNotifyState(
                shop_label="MINMIN",
                last_action="resolved",
                last_sent_at="2026-02-27T05:00:00Z",
                resolved_cooldown_until="2026-02-27T06:00:00Z",
            )
        )
        session.commit()
    finally:
        session.close()

    payload = cli._build_phase1_status_payload_for_cli(
        shops_value="samord,minmin",
        reports_dir=str(reports_root),
    )
    doctor_notify = payload.get("doctor_notify") or {}
    assert doctor_notify["samord"]["last_action"] == "alert"
    assert doctor_notify["samord"]["last_sent_at"] is None
    assert doctor_notify["samord"]["cooldown_until"] == "2026-02-27T04:00:00Z"
    assert doctor_notify["minmin"]["last_action"] == "resolved"
    assert doctor_notify["minmin"]["last_sent_at"] == "2026-02-27T05:00:00Z"
    assert doctor_notify["minmin"]["resolved_cooldown_until"] == "2026-02-27T06:00:00Z"
    assert doctor_notify["samord"]["last_action"] in {"alert", "resolved", "ok"}
    assert doctor_notify["minmin"]["last_action"] in {"alert", "resolved", "ok"}
    get_settings.cache_clear()


def test_status_dump_ads_rate_limit_config_and_missing_parent_issue(
    monkeypatch, tmp_path: Path
) -> None:
    reports_root = _configure_test_env(monkeypatch, tmp_path)
    now_utc = datetime.now(timezone.utc)
    _seed_tokens_and_ads(now_utc)
    missing_parent = tmp_path / "missing_parent_dir"
    monkeypatch.setenv(
        "DOTORI_ADS_RATE_LIMIT_STATE_PATH",
        str(missing_parent / "ads_rate_limit_state.json"),
    )
    get_settings.cache_clear()

    payload = cli._build_phase1_status_payload_for_cli(
        shops_value="samord,minmin",
        reports_dir=str(reports_root),
    )
    config = payload.get("ads_rate_limit_config") or {}
    assert config["state_path_source"] == "env"
    assert config["parent_dir_exists"] is False
    assert config["parent_dir_writable"] is False
    assert "parent_missing=" in str(config.get("writable_probe_error") or "")

    issues = [row for row in payload.get("issues", []) if isinstance(row, dict)]
    assert any(
        row.get("shop") == "samord"
        and row.get("code") == "ADS_RATE_LIMIT_STATE_PATH_PARENT_MISSING"
        and row.get("severity") == "warn"
        for row in issues
    )
    assert any(
        row.get("shop") == "minmin"
        and row.get("code") == "ADS_RATE_LIMIT_STATE_PATH_PARENT_MISSING"
        and row.get("severity") == "warn"
        for row in issues
    )
    get_settings.cache_clear()


def test_doctor_summary_lines_include_ads_rate_limit_state_triplet() -> None:
    payload = {
        "shops": ["samord", "minmin"],
        "token": {},
        "db": {"latest_ingest": {}},
        "reports": {"latest": {}},
        "issues": [],
        "ads_rate_limit_config": {
            "state_path_effective": "/var/lib/dotori_shopee_automation/ads_rate_limit_state.json",
            "parent_dir_exists": True,
            "parent_dir_writable": True,
        },
    }
    lines = cli._build_phase1_doctor_summary_lines(payload=payload, max_issues=5)
    target = [
        line for line in lines if line.startswith("ads_rate_limit_state ")
    ]
    assert len(target) == 1
    line = target[0]
    assert "ads_rate_limit_state_path=/var/lib/dotori_shopee_automation/ads_rate_limit_state.json" in line
    assert "ads_rate_limit_state_path_exists=1" in line
    assert "ads_rate_limit_state_path_writable=1" in line
