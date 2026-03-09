from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.db import SessionLocal, init_db
from dotori_shopee_automation.shopee.token_store import upsert_token


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = REPO_ROOT / "tests" / "fixtures" / "shopee_ads"


def _write_shops(path: Path, include_shop_id: bool = True) -> None:
    lines = [
        "- shop_key: samord",
        "  label: SAMORD",
        "  enabled: true",
    ]
    if include_shop_id:
        lines.append("  shopee_shop_id: 111")
    lines += [
        "- shop_key: minmin",
        "  label: MINMIN",
        "  enabled: true",
    ]
    if include_shop_id:
        lines.append("  shopee_shop_id: 222")
    path.write_text("\n".join(lines), encoding="utf-8")


def _seed_tokens(db_url: str, access: str = "ACCESS_TOKEN", refresh: str = "REFRESH_TOKEN") -> None:
    prev_db = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = db_url
    get_settings.cache_clear()
    init_db()
    session = SessionLocal()
    try:
        now = datetime.now(timezone.utc) + timedelta(hours=1)
        upsert_token(session, "samord", 111, access, refresh, now)
        upsert_token(session, "minmin", 222, access, refresh, now)
        session.commit()
    finally:
        session.close()
    if prev_db is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = prev_db
    get_settings.cache_clear()


def test_phase1_verify_not_ready(tmp_path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)
    db_path = tmp_path / "verify.db"

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env["DATABASE_URL"] = f"sqlite:///{db_path}"
    env.pop("DISCORD_WEBHOOK_REPORT_URL", None)
    env.pop("DISCORD_WEBHOOK_ALERTS_URL", None)
    env.pop("SHOPEE_PARTNER_ID", None)
    env.pop("SHOPEE_PARTNER_KEY", None)

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "verify",
        "--shops",
        "samord,minmin",
    ]

    result = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    stdout = result.stdout
    assert "ready=0" in stdout
    assert "discord_report_webhook" in stdout
    assert "discord_alerts_webhook" in stdout
    assert "shopee_partner_id" in stdout
    assert "shopee_partner_key" in stdout


def test_phase1_verify_ready(tmp_path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path, include_shop_id=False)
    db_path = tmp_path / "verify_ready.db"
    db_url = f"sqlite:///{db_path}"

    _seed_tokens(db_url)

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env["DATABASE_URL"] = db_url
    env["DISCORD_WEBHOOK_REPORT_URL"] = "http://example.invalid/report"
    env["DISCORD_WEBHOOK_ALERTS_URL"] = "http://example.invalid/alerts"
    env["SHOPEE_PARTNER_ID"] = "999"
    env["SHOPEE_PARTNER_KEY"] = "PARTNER_KEY_SHOULD_NOT_PRINT"
    env["SHOPEE_SAMORD_SHOP_ID"] = "123"
    env["SHOPEE_MINMIN_SHOP_ID"] = "456"
    env["ADS_DAILY_PATH"] = "/api/v2/marketing/report/daily"
    env["ADS_SNAPSHOT_PATH"] = "/api/v2/marketing/report/snapshot"

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "verify",
        "--shops",
        "samord,minmin",
        "--fixtures-dir",
        str(FIXTURE_ROOT),
    ]

    result = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    stdout = result.stdout
    assert "ready=1" in stdout
    assert "phase1_verify ready=1" in stdout
    assert "ping=fixtures" in stdout


def test_phase1_verify_ping_live_without_allow_network(tmp_path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path, include_shop_id=False)
    db_path = tmp_path / "verify_live.db"
    db_url = f"sqlite:///{db_path}"

    _seed_tokens(db_url)

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env["DATABASE_URL"] = db_url
    env["DISCORD_WEBHOOK_REPORT_URL"] = "http://example.invalid/report"
    env["DISCORD_WEBHOOK_ALERTS_URL"] = "http://example.invalid/alerts"
    env["SHOPEE_PARTNER_ID"] = "999"
    env["SHOPEE_PARTNER_KEY"] = "PARTNER_KEY_SHOULD_NOT_PRINT"
    env["SHOPEE_SAMORD_SHOP_ID"] = "123"
    env["SHOPEE_MINMIN_SHOP_ID"] = "456"
    env["ADS_DAILY_PATH"] = "/api/v2/marketing/report/daily"
    env["ADS_SNAPSHOT_PATH"] = "/api/v2/marketing/report/snapshot"

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "verify",
        "--shops",
        "samord,minmin",
        "--ping-live",
        "--fixtures-dir",
        str(FIXTURE_ROOT),
    ]

    result = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    stdout = result.stdout
    assert "allow_network_required" in stdout or "network_disabled" in stdout


def test_phase1_verify_never_prints_secrets(tmp_path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)
    db_path = tmp_path / "verify_secret.db"
    db_url = f"sqlite:///{db_path}"

    _seed_tokens(db_url, access="ACCESS_TOKEN_SHOULD_NOT_PRINT", refresh="REFRESH_TOKEN_SHOULD_NOT_PRINT")

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env["DATABASE_URL"] = db_url
    env["DISCORD_WEBHOOK_REPORT_URL"] = "http://example.invalid/report"
    env["DISCORD_WEBHOOK_ALERTS_URL"] = "http://example.invalid/alerts"
    env["SHOPEE_PARTNER_ID"] = "999"
    env["SHOPEE_PARTNER_KEY"] = "PARTNER_KEY_SHOULD_NOT_PRINT"

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "verify",
        "--shops",
        "samord,minmin",
        "--fixtures-dir",
        str(FIXTURE_ROOT),
    ]

    result = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    stdout = result.stdout
    assert "ACCESS_TOKEN_SHOULD_NOT_PRINT" not in stdout
    assert "REFRESH_TOKEN_SHOULD_NOT_PRINT" not in stdout
    assert "PARTNER_KEY_SHOULD_NOT_PRINT" not in stdout
