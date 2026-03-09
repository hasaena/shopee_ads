from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json
import os
import re
from pathlib import Path

from fastapi.testclient import TestClient

from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.ads.models import AdsCampaign, AdsCampaignDaily, AdsCampaignSnapshot
from dotori_shopee_automation.db import EventLog, SessionLocal, init_db
from dotori_shopee_automation.shopee.token_store import upsert_token
from dotori_shopee_automation import webapp
from dotori_shopee_automation.webapp import app


def _write_phase1_shops(path: Path) -> None:
    path.write_text(
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
                "- shop_key: extra",
                "  label: EXTRA",
                "  enabled: true",
                "  shopee_shop_id: 999999999",
            ]
        ),
        encoding="utf-8",
    )


def _configure_test_env(monkeypatch, tmp_path: Path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_phase1_shops(shops_path)
    db_path = tmp_path / "phase1_status.db"
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path.as_posix()}")
    monkeypatch.setenv("REPORTS_DIR", str(reports_dir))
    monkeypatch.setenv("OPS_TOKEN", "ops-secret")
    monkeypatch.setenv("SHOPEE_SAMORD_SHOP_ID", "497412318")
    monkeypatch.setenv("SHOPEE_MINMIN_SHOP_ID", "567655304")
    monkeypatch.setenv("REPORT_BASE_URL", "http://localhost:8000")
    monkeypatch.setenv("REPORT_ACCESS_TOKEN", "TEST_TOKEN_DO_NOT_USE")
    get_settings.cache_clear()


def _seed_tokens_and_gate_status(
    now: datetime | None = None,
    *,
    include_minmin_token: bool = True,
) -> None:
    base = now or datetime.now(timezone.utc)
    init_db()
    session = SessionLocal()
    try:
        upsert_token(
            session,
            "samord",
            497412318,
            "SENSITIVE_ACCESS_TOKEN_SAMORD_1234567890",
            "SENSITIVE_REFRESH_TOKEN_SAMORD_1234567890",
            base + timedelta(minutes=30),
            base + timedelta(days=30),
        )
        if include_minmin_token:
            upsert_token(
                session,
                "minmin",
                567655304,
                "SENSITIVE_ACCESS_TOKEN_MINMIN_1234567890",
                "SENSITIVE_REFRESH_TOKEN_MINMIN_1234567890",
                base + timedelta(minutes=60),
                base + timedelta(days=30),
            )
        session.add(
            EventLog(
                level="INFO",
                message="token_preflight_gate_status",
                meta_json=json.dumps(
                    {
                        "shop_key": "samord",
                        "shop_label": "SAMORD",
                        "shop_id": 497412318,
                        "last_verdict": "ok",
                        "last_alert_at": -1,
                        "last_resolved_at": -1,
                        "cooldown_until": int(base.timestamp()) + 1200,
                        "resolved_cooldown_until": -1,
                        "min_required_ttl_sec": 1200,
                        "last_access_ttl_sec": 1800,
                        "gate_state": "ok",
                        "updated_at": int(base.timestamp()),
                    },
                    ensure_ascii=True,
                ),
            )
        )
        session.add(
            EventLog(
                level="INFO",
                message="token_preflight_gate_status",
                meta_json=json.dumps(
                    {
                        "shop_key": "minmin",
                        "shop_label": "MINMIN",
                        "shop_id": 567655304,
                        "last_verdict": "ok",
                        "last_alert_at": -1,
                        "last_resolved_at": -1,
                        "cooldown_until": int(base.timestamp()) + 1200,
                        "resolved_cooldown_until": int(base.timestamp()) + 1800,
                        "min_required_ttl_sec": 1200,
                        "last_access_ttl_sec": 3600,
                        "gate_state": "ok",
                        "updated_at": int(base.timestamp()),
                    },
                    ensure_ascii=True,
                ),
            )
        )
        session.commit()
    finally:
        session.close()


def _seed_ads_freshness_rows(
    now: datetime | None = None,
    *,
    samord_daily_date: date | None = None,
    minmin_daily_date: date | None = None,
    samord_snapshot_age_minutes: int = 10,
    minmin_snapshot_age_minutes: int = 5,
) -> None:
    base = now or datetime.now(timezone.utc)
    samord_daily = samord_daily_date or date(2026, 2, 25)
    minmin_daily = minmin_daily_date or date(2026, 2, 24)
    init_db()
    session = SessionLocal()
    try:
        session.add(
            AdsCampaign(
                shop_key="samord",
                campaign_id="c_samord_1",
                campaign_name="Samord Campaign 1",
                status="RUNNING",
                daily_budget=100.0,
            )
        )
        session.add(
            AdsCampaign(
                shop_key="minmin",
                campaign_id="c_minmin_1",
                campaign_name="Minmin Campaign 1",
                status="RUNNING",
                daily_budget=120.0,
            )
        )
        session.add(
            AdsCampaignDaily(
                shop_key="samord",
                campaign_id="c_samord_1",
                date=samord_daily,
                spend=20.0,
                impressions=1000,
                clicks=100,
                orders=3,
                gmv=80.0,
            )
        )
        session.add(
            AdsCampaignDaily(
                shop_key="minmin",
                campaign_id="c_minmin_1",
                date=minmin_daily,
                spend=30.0,
                impressions=1200,
                clicks=120,
                orders=4,
                gmv=90.0,
            )
        )
        session.add(
            AdsCampaignSnapshot(
                shop_key="samord",
                campaign_id="c_samord_1",
                ts=base - timedelta(minutes=samord_snapshot_age_minutes),
                spend_today=20.0,
                impressions_today=1000,
                clicks_today=100,
                orders_today=3,
                gmv_today=80.0,
            )
        )
        session.add(
            AdsCampaignSnapshot(
                shop_key="minmin",
                campaign_id="c_minmin_1",
                ts=base - timedelta(minutes=minmin_snapshot_age_minutes),
                spend_today=30.0,
                impressions_today=1200,
                clicks_today=120,
                orders_today=4,
                gmv_today=90.0,
            )
        )
        session.commit()
    finally:
        session.close()


def _write_report_file(path: Path, text: str, mtime: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    os.utime(path, (mtime, mtime))


def _seed_reports(
    tmp_path: Path,
    *,
    now_utc: datetime | None = None,
    age_hours: int = 1,
) -> None:
    reports_root = tmp_path / "reports"
    base = now_utc or datetime.now(timezone.utc)
    base_ts = int((base - timedelta(hours=age_hours)).timestamp())
    # samord: midday + final, no weekly
    _write_report_file(
        reports_root / "samord" / "daily" / "2026-02-25_midday.html",
        "<html>samord midday</html>",
        base_ts,
    )
    _write_report_file(
        reports_root / "samord" / "daily" / "2026-02-24_final.html",
        "<html>samord final</html>",
        base_ts - 120,
    )
    # minmin: midday + final + weekly
    _write_report_file(
        reports_root / "minmin" / "daily" / "2026-02-25_midday.html",
        "<html>minmin midday</html>",
        base_ts + 30,
    )
    _write_report_file(
        reports_root / "minmin" / "daily" / "2026-02-24_final.html",
        "<html>minmin final</html>",
        base_ts - 90,
    )
    _write_report_file(
        reports_root / "minmin" / "weekly" / "2026-W08.html",
        "<html>minmin weekly</html>",
        base_ts - 180,
    )


def test_phase1_status_requires_auth(tmp_path: Path, monkeypatch) -> None:
    _configure_test_env(monkeypatch, tmp_path)
    with TestClient(app) as client:
        response = client.get("/ops/phase1/status")

    assert response.status_code in {401, 403}
    assert response.headers.get("Cache-Control") == "no-store"
    assert response.headers.get("X-Content-Type-Options") == "nosniff"
    get_settings.cache_clear()


def test_phase1_status_safe_schema_and_no_secrets(tmp_path: Path, monkeypatch) -> None:
    _configure_test_env(monkeypatch, tmp_path)
    _seed_tokens_and_gate_status()

    with TestClient(app) as client:
        response = client.get(
            "/ops/phase1/status",
            headers={"Authorization": "Bearer ops-secret"},
        )

    assert response.status_code == 200, response.text
    assert response.headers.get("Cache-Control") == "no-store"
    assert response.headers.get("X-Content-Type-Options") == "nosniff"

    data = response.json()
    assert data.get("ok") is True
    assert data.get("phase") == "phase1"
    assert data.get("timezone") == "Asia/Ho_Chi_Minh"
    assert sorted(data.get("shops") or []) == ["minmin", "samord"]
    assert "db" in data
    assert "reports" in data
    assert "doctor_notify" in data
    assert "freshness" in data
    assert "issues" in data

    token_payload = data.get("token") or {}
    assert sorted(token_payload.keys()) == ["minmin", "samord"]
    for shop_key in ("samord", "minmin"):
        row = token_payload[shop_key]
        assert set(row.keys()) == {
            "shop_id",
            "token_len",
            "token_sha8",
            "access_expires_in_sec",
            "expires_in_sec",
            "updated_at",
            "gate_state",
            "cooldown_until",
            "resolved_cooldown_until",
            "token_source",
            "token_mode",
            "has_refresh_token",
            "refresh_expires_in_sec",
            "token_import_last_at",
            "token_import_last_request_id",
            "next_action",
        }

    body = response.text
    forbidden_literals = [
        '"access_token":',
        '"refresh_token":',
        "partner_key",
        "discord.com/api/webhooks",
        "SENSITIVE_ACCESS_TOKEN",
        "SENSITIVE_REFRESH_TOKEN",
    ]
    for literal in forbidden_literals:
        assert literal not in body

    assert re.search(r"Bearer\\s+[A-Za-z0-9._-]{20,}", body) is None
    get_settings.cache_clear()


def test_phase1_status_schedule_basic(monkeypatch, tmp_path: Path) -> None:
    _configure_test_env(monkeypatch, tmp_path)
    fixed_now = datetime(2026, 2, 25, 5, 7, 12, tzinfo=timezone.utc)
    _seed_tokens_and_gate_status(now=fixed_now)
    monkeypatch.setattr(webapp, "_now_utc", lambda: fixed_now)

    with TestClient(app) as client:
        response = client.get(
            "/ops/phase1/status",
            headers={"Authorization": "Bearer ops-secret"},
        )

    assert response.status_code == 200, response.text
    schedule = response.json()["schedule"]
    assert schedule["daily_final"]["local"] == "2026-02-26 00:00"
    assert schedule["daily_midday"]["local"] == "2026-02-25 13:00"
    assert schedule["weekly"]["local"] == "2026-03-02 09:00"
    assert schedule["alerts_15m"]["local"] == "2026-02-25 12:15"
    assert schedule["is_business_hours"] is True
    get_settings.cache_clear()


def test_phase1_status_db_freshness_and_report_pointers(monkeypatch, tmp_path: Path) -> None:
    _configure_test_env(monkeypatch, tmp_path)
    fixed_now = datetime(2026, 2, 25, 5, 7, 12, tzinfo=timezone.utc)
    _seed_tokens_and_gate_status(now=fixed_now)
    _seed_ads_freshness_rows(now=fixed_now)
    _seed_reports(tmp_path, now_utc=fixed_now, age_hours=1)
    monkeypatch.setattr(webapp, "_now_utc", lambda: fixed_now)

    with TestClient(app) as client:
        response = client.get(
            "/ops/phase1/status",
            headers={"Authorization": "Bearer ops-secret"},
        )

    assert response.status_code == 200, response.text
    data = response.json()

    db = data["db"]
    assert db["ok"] is True
    assert db["engine"] == "sqlite"
    assert db["row_counts"]["ads_campaign"] >= 2
    assert db["row_counts"]["ads_daily"] >= 2
    assert db["row_counts"]["ads_snapshot"] >= 2
    assert db["latest_ingest"]["samord"]["daily_latest_date"] == "2026-02-25"
    assert db["latest_ingest"]["minmin"]["daily_latest_date"] == "2026-02-24"
    assert db["latest_ingest"]["samord"]["snapshot_latest_at"] is not None
    assert db["latest_ingest"]["minmin"]["snapshot_latest_at"] is not None
    assert db["latest_ingest"]["samord"]["snapshot_latest_at"].endswith("+07:00")

    reports = data["reports"]
    assert reports["base_url"] == "http://localhost:8000"
    latest = reports["latest"]
    assert sorted(latest.keys()) == ["minmin", "samord"]

    samord_midday = latest["samord"]["daily_midday"]
    assert samord_midday["relpath"] == "samord/daily/2026-02-25_midday.html"
    assert isinstance(samord_midday["size"], int) and samord_midday["size"] > 0
    assert samord_midday["updated_at"] is not None
    assert isinstance(samord_midday["age_hours"], float)
    assert samord_midday["is_stale"] is False
    assert samord_midday["url"] is not None
    assert "token=***" in samord_midday["url"]
    assert "TEST_TOKEN_DO_NOT_USE" not in samord_midday["url"]
    assert ":" not in samord_midday["relpath"].split("/")[0]

    samord_weekly = latest["samord"]["weekly"]
    assert samord_weekly is None

    minmin_weekly = latest["minmin"]["weekly"]
    assert minmin_weekly["relpath"] == "minmin/weekly/2026-W08.html"
    assert "token=***" in minmin_weekly["url"]
    assert minmin_weekly["is_stale"] is False

    freshness = data["freshness"]
    assert freshness["thresholds"] == {
        "daily_stale_after_days": 2,
        "snapshot_stale_after_minutes": 90,
        "report_stale_after_hours": 48,
    }
    assert freshness["per_shop"]["samord"]["daily_is_stale"] is False
    assert freshness["per_shop"]["samord"]["snapshot_is_stale"] is False
    assert freshness["per_shop"]["samord"]["reports_is_stale"] is False
    assert freshness["per_shop"]["samord"]["reports_detail"] == {
        "daily_midday_is_stale": False,
        "daily_final_is_stale": False,
        "weekly_is_stale": False,
    }
    issue_codes = {(row["shop"], row["code"]) for row in data["issues"]}
    assert ("samord", "REPORT_MISSING_WEEKLY") in issue_codes
    get_settings.cache_clear()


def test_phase1_status_reports_url_is_null_without_base_url(monkeypatch, tmp_path: Path) -> None:
    _configure_test_env(monkeypatch, tmp_path)
    monkeypatch.delenv("REPORT_BASE_URL", raising=False)
    get_settings.cache_clear()
    _seed_tokens_and_gate_status()
    _seed_reports(tmp_path)

    with TestClient(app) as client:
        response = client.get(
            "/ops/phase1/status",
            headers={"Authorization": "Bearer ops-secret"},
        )

    assert response.status_code == 200, response.text
    reports = response.json()["reports"]
    assert reports["base_url"] is None
    for shop_key in ("samord", "minmin"):
        for kind in ("daily_midday", "daily_final", "weekly"):
            pointer = reports["latest"][shop_key][kind]
            if pointer is None:
                continue
            assert pointer["relpath"] is not None
            assert pointer["url"] is None
    get_settings.cache_clear()


def test_phase1_status_completeness_and_token_missing_issue(monkeypatch, tmp_path: Path) -> None:
    _configure_test_env(monkeypatch, tmp_path)
    fixed_now = datetime(2026, 2, 25, 5, 7, 12, tzinfo=timezone.utc)
    _seed_tokens_and_gate_status(now=fixed_now, include_minmin_token=False)
    monkeypatch.setattr(webapp, "_now_utc", lambda: fixed_now)

    with TestClient(app) as client:
        response = client.get(
            "/ops/phase1/status",
            headers={"Authorization": "Bearer ops-secret"},
        )

    assert response.status_code == 200, response.text
    data = response.json()
    assert sorted(data["token"].keys()) == ["minmin", "samord"]
    assert sorted(data["db"]["latest_ingest"].keys()) == ["minmin", "samord"]
    assert sorted(data["reports"]["latest"].keys()) == ["minmin", "samord"]
    assert sorted(data["doctor_notify"].keys()) == ["minmin", "samord"]
    assert sorted(data["freshness"]["per_shop"].keys()) == ["minmin", "samord"]

    assert data["token"]["minmin"]["token_len"] == 0
    assert data["token"]["minmin"]["token_sha8"] == "-"
    token_missing_issues = [
        row for row in data["issues"] if row["shop"] == "minmin" and row["code"] == "TOKEN_MISSING"
    ]
    assert len(token_missing_issues) == 1
    assert token_missing_issues[0]["severity"] == "error"
    get_settings.cache_clear()


def test_phase1_status_report_missing_issue_matrix(monkeypatch, tmp_path: Path) -> None:
    _configure_test_env(monkeypatch, tmp_path)
    fixed_now = datetime(2026, 2, 25, 5, 7, 12, tzinfo=timezone.utc)
    _seed_tokens_and_gate_status(now=fixed_now)
    _seed_ads_freshness_rows(now=fixed_now)
    reports_root = tmp_path / "reports"
    # Seed only one report to force missing matrix generation for both shops/jobs.
    _write_report_file(
        reports_root / "samord" / "daily" / "2026-02-25_midday.html",
        "<html>only one report</html>",
        int(fixed_now.timestamp()),
    )
    monkeypatch.setattr(webapp, "_now_utc", lambda: fixed_now)

    with TestClient(app) as client:
        response = client.get(
            "/ops/phase1/status",
            headers={"Authorization": "Bearer ops-secret"},
        )

    assert response.status_code == 200, response.text
    data = response.json()
    reports_latest = data["reports"]["latest"]
    assert sorted(reports_latest.keys()) == ["minmin", "samord"]
    for shop_key in ("samord", "minmin"):
        assert set(reports_latest[shop_key].keys()) == {
            "daily_midday",
            "daily_final",
            "weekly",
        }

    issues = {(row["shop"], row["code"], row["severity"]) for row in data["issues"]}
    assert ("samord", "REPORT_MISSING_DAILY_FINAL", "warn") in issues
    assert ("samord", "REPORT_MISSING_WEEKLY", "info") in issues
    assert ("minmin", "REPORT_MISSING_DAILY_MIDDAY", "warn") in issues
    assert ("minmin", "REPORT_MISSING_DAILY_FINAL", "warn") in issues
    assert ("minmin", "REPORT_MISSING_WEEKLY", "info") in issues
    get_settings.cache_clear()


def test_phase1_status_freshness_flags_and_issues_for_stale_data(monkeypatch, tmp_path: Path) -> None:
    _configure_test_env(monkeypatch, tmp_path)
    fixed_now = datetime(2026, 2, 25, 5, 7, 12, tzinfo=timezone.utc)
    monkeypatch.setenv("DOTORI_STATUS_DAILY_STALE_AFTER_DAYS", "2")
    monkeypatch.setenv("DOTORI_STATUS_SNAPSHOT_STALE_AFTER_MINUTES", "90")
    monkeypatch.setenv("DOTORI_STATUS_REPORT_STALE_AFTER_HOURS", "48")
    get_settings.cache_clear()

    _seed_tokens_and_gate_status(now=fixed_now)
    _seed_ads_freshness_rows(
        now=fixed_now,
        samord_daily_date=date(2026, 2, 20),
        minmin_daily_date=date(2026, 2, 19),
        samord_snapshot_age_minutes=300,
        minmin_snapshot_age_minutes=240,
    )
    _seed_reports(tmp_path, now_utc=fixed_now, age_hours=96)
    monkeypatch.setattr(webapp, "_now_utc", lambda: fixed_now)

    with TestClient(app) as client:
        response = client.get(
            "/ops/phase1/status",
            headers={"Authorization": "Bearer ops-secret"},
        )

    assert response.status_code == 200, response.text
    data = response.json()
    for shop_key in ("samord", "minmin"):
        row = data["freshness"]["per_shop"][shop_key]
        assert row["daily_is_stale"] is True
        assert row["snapshot_is_stale"] is True
        assert row["reports_is_stale"] is True
        assert row["reports_detail"]["daily_midday_is_stale"] is True
        assert row["reports_detail"]["daily_final_is_stale"] is True
    assert data["freshness"]["per_shop"]["samord"]["reports_detail"]["weekly_is_stale"] is False
    assert data["freshness"]["per_shop"]["minmin"]["reports_detail"]["weekly_is_stale"] is True

    issue_codes = {(row["shop"], row["code"]) for row in data["issues"]}
    assert ("samord", "DAILY_STALE") in issue_codes
    assert ("samord", "SNAPSHOT_STALE") in issue_codes
    assert ("samord", "REPORT_STALE_DAILY_MIDDAY") in issue_codes
    assert ("samord", "REPORT_STALE_DAILY_FINAL") in issue_codes
    assert ("samord", "REPORT_MISSING_WEEKLY") in issue_codes
    assert ("minmin", "DAILY_STALE") in issue_codes
    assert ("minmin", "SNAPSHOT_STALE") in issue_codes
    assert ("minmin", "REPORT_STALE_DAILY_MIDDAY") in issue_codes
    assert ("minmin", "REPORT_STALE_DAILY_FINAL") in issue_codes
    assert ("minmin", "REPORT_STALE_WEEKLY") in issue_codes
    get_settings.cache_clear()


def test_phase1_status_report_latest_prefers_report_date_over_mtime(
    monkeypatch, tmp_path: Path
) -> None:
    _configure_test_env(monkeypatch, tmp_path)
    fixed_now = datetime(2026, 3, 9, 5, 7, 12, tzinfo=timezone.utc)
    _seed_tokens_and_gate_status(now=fixed_now)
    _seed_ads_freshness_rows(now=fixed_now)
    reports_root = tmp_path / "reports"

    # Old date file has newer mtime (simulating re-render of old final).
    _write_report_file(
        reports_root / "minmin" / "daily" / "2026-03-05_final.html",
        "<html>old-date-but-newer-mtime</html>",
        int((fixed_now + timedelta(hours=1)).timestamp()),
    )
    # Newest final date has older mtime.
    _write_report_file(
        reports_root / "minmin" / "daily" / "2026-03-08_final.html",
        "<html>newest-date</html>",
        int((fixed_now - timedelta(hours=1)).timestamp()),
    )
    # Keep samord minimally populated to avoid report-missing noise side effects.
    _write_report_file(
        reports_root / "samord" / "daily" / "2026-03-08_final.html",
        "<html>samord final</html>",
        int((fixed_now - timedelta(hours=1)).timestamp()),
    )

    monkeypatch.setattr(webapp, "_now_utc", lambda: fixed_now)

    with TestClient(app) as client:
        response = client.get(
            "/ops/phase1/status",
            headers={"Authorization": "Bearer ops-secret"},
        )

    assert response.status_code == 200, response.text
    data = response.json()
    assert (
        data["reports"]["latest"]["minmin"]["daily_final"]["relpath"]
        == "minmin/daily/2026-03-08_final.html"
    )
    get_settings.cache_clear()


def test_phase1_status_report_lag_daily_final_issue(monkeypatch, tmp_path: Path) -> None:
    _configure_test_env(monkeypatch, tmp_path)
    fixed_now = datetime(2026, 2, 25, 5, 7, 12, tzinfo=timezone.utc)  # local 12:07
    _seed_tokens_and_gate_status(now=fixed_now)
    _seed_ads_freshness_rows(now=fixed_now)
    reports_root = tmp_path / "reports"

    _write_report_file(
        reports_root / "samord" / "daily" / "2026-02-20_final.html",
        "<html>lagged final</html>",
        int((fixed_now - timedelta(minutes=5)).timestamp()),
    )
    _write_report_file(
        reports_root / "samord" / "daily" / "2026-02-25_midday.html",
        "<html>current midday</html>",
        int((fixed_now - timedelta(minutes=5)).timestamp()),
    )
    _write_report_file(
        reports_root / "minmin" / "daily" / "2026-02-24_final.html",
        "<html>normal final</html>",
        int((fixed_now - timedelta(minutes=5)).timestamp()),
    )

    monkeypatch.setattr(webapp, "_now_utc", lambda: fixed_now)

    with TestClient(app) as client:
        response = client.get(
            "/ops/phase1/status",
            headers={"Authorization": "Bearer ops-secret"},
        )

    assert response.status_code == 200, response.text
    data = response.json()
    issues = {(row["shop"], row["code"]) for row in data["issues"]}
    assert ("samord", "REPORT_LAG_DAILY_FINAL") in issues
    assert ("minmin", "REPORT_LAG_DAILY_FINAL") not in issues
    get_settings.cache_clear()


def test_phase1_status_report_relpath_traversal_prevention(tmp_path: Path) -> None:
    reports_root = tmp_path / "reports"
    reports_root.mkdir(parents=True, exist_ok=True)
    outside_file = tmp_path / "outside" / "evil.html"
    outside_file.parent.mkdir(parents=True, exist_ok=True)
    outside_file.write_text("<html>evil</html>", encoding="utf-8")

    pointer = webapp._build_report_pointer(
        reports_root=reports_root,
        file_path=outside_file,
        now_utc=datetime.now(timezone.utc),
        stale_after_hours=48,
    )
    assert pointer is None


def test_phase1_status_token_payload_matches_helper(monkeypatch, tmp_path: Path) -> None:
    _configure_test_env(monkeypatch, tmp_path)
    fixed_now = datetime(2026, 2, 25, 5, 7, 12, tzinfo=timezone.utc)
    _seed_tokens_and_gate_status(now=fixed_now)
    monkeypatch.setattr(webapp, "_now_utc", lambda: fixed_now)

    with TestClient(app) as client:
        response = client.get(
            "/ops/phase1/status",
            headers={"Authorization": "Bearer ops-secret"},
        )

    assert response.status_code == 200, response.text
    assert response.json()["token"] == webapp._build_phase1_token_status_payload()
    get_settings.cache_clear()


def test_phase1_ops_endpoints_set_no_store_headers(tmp_path: Path, monkeypatch) -> None:
    _configure_test_env(monkeypatch, tmp_path)
    _seed_tokens_and_gate_status()
    payload = {
        "tokens": {
            "497412318": {
                "access_token": "A" * 32,
                "refresh_token": "R" * 33,
                "expire_timestamp": 1_893_456_000,
                "refresh_token_expire_timestamp": 1_896_048_000,
            },
            "567655304": {
                "access_token": "B" * 32,
                "refresh_token": "S" * 33,
                "expire_timestamp": 1_893_456_000,
                "refresh_token_expire_timestamp": 1_896_048_000,
            },
        }
    }

    with TestClient(app) as client:
        ping = client.get(
            "/ops/phase1/token/ping",
            headers={"Authorization": "Bearer ops-secret"},
        )
        token_status = client.get(
            "/ops/phase1/token/status",
            headers={"Authorization": "Bearer ops-secret"},
        )
        imported = client.post(
            "/ops/phase1/token/import",
            json=payload,
            headers={"Authorization": "Bearer ops-secret"},
        )
        status = client.get(
            "/ops/phase1/status",
            headers={"Authorization": "Bearer ops-secret"},
        )

    for response in (ping, token_status, imported, status):
        assert response.status_code == 200, response.text
        assert response.headers.get("Cache-Control") == "no-store"
        assert response.headers.get("X-Content-Type-Options") == "nosniff"
    get_settings.cache_clear()
