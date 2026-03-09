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


def _seed_tokens(db_url: str, include_secrets: bool = False) -> None:
    prev_db = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = db_url
    get_settings.cache_clear()
    init_db()
    session = SessionLocal()
    try:
        now = datetime.now(timezone.utc) + timedelta(hours=1)
        access = "ACCESS_TOKEN"
        refresh = "REFRESH_TOKEN"
        if include_secrets:
            access = "FAKE_ACCESS_TOKEN_SHOULD_NOT_PRINT"
            refresh = "FAKE_REFRESH_TOKEN_SHOULD_NOT_PRINT"
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


def test_ops_readiness_not_ready(tmp_path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path, include_shop_id=True)
    db_path = tmp_path / "readiness.db"

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env["DATABASE_URL"] = f"sqlite:///{db_path}"
    env.pop("DISCORD_WEBHOOK_REPORT_URL", None)
    env.pop("DISCORD_WEBHOOK_ALERTS_URL", None)
    env.pop("DISCORD_WEBHOOK_ACTIONS_URL", None)
    env.pop("ADS_DAILY_PATH", None)
    env.pop("ADS_SNAPSHOT_PATH", None)

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "readiness",
        "phase1",
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
    assert "readiness_phase1 shops=samord,minmin" in stdout
    assert "discord_webhooks report=0 alerts=0" in stdout
    assert "shopee_credentials partner_id=0 partner_key=0" in stdout
    assert "ads_endpoints daily_path=0 snapshot_path=0" in stdout
    assert "ready=0" in stdout
    assert "discord_report_webhook" in stdout
    assert "discord_alerts_webhook" in stdout
    assert "shopee_partner_id" in stdout
    assert "shopee_partner_key" in stdout
    assert "samord_token_access" in stdout
    assert "samord_token_refresh" in stdout
    assert "minmin_token_access" in stdout
    assert "minmin_token_refresh" in stdout


def test_ops_readiness_ready_with_tokens(tmp_path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path, include_shop_id=False)
    db_path = tmp_path / "readiness_ready.db"
    db_url = f"sqlite:///{db_path}"

    _seed_tokens(db_url, include_secrets=False)

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env["DATABASE_URL"] = db_url
    env["DISCORD_WEBHOOK_REPORT_URL"] = "http://example.invalid/report"
    env["DISCORD_WEBHOOK_ALERTS_URL"] = "http://example.invalid/alerts"
    env["DISCORD_WEBHOOK_ACTIONS_URL"] = "http://example.invalid/actions"
    env["SHOPEE_PARTNER_ID"] = "999"
    env["SHOPEE_PARTNER_KEY"] = "PARTNER_KEY_SHOULD_NOT_PRINT"
    env["SHOPEE_SAMORD_SHOP_ID"] = "123"
    env["SHOPEE_MINMIN_SHOP_ID"] = "456"
    env.pop("ADS_DAILY_PATH", None)
    env.pop("ADS_SNAPSHOT_PATH", None)

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "readiness",
        "phase1",
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

    assert result.returncode == 0, result.stdout + result.stderr
    stdout = result.stdout
    assert "ready=1" in stdout


def test_ops_readiness_never_prints_secrets(tmp_path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path, include_shop_id=True)
    db_path = tmp_path / "readiness_secrets.db"
    db_url = f"sqlite:///{db_path}"

    _seed_tokens(db_url, include_secrets=True)

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env["DATABASE_URL"] = db_url
    env["SHOPEE_PARTNER_ID"] = "999"
    env["SHOPEE_PARTNER_KEY"] = "PARTNER_KEY_SHOULD_NOT_PRINT"
    env.pop("ADS_DAILY_PATH", None)
    env.pop("ADS_SNAPSHOT_PATH", None)

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "readiness",
        "phase1",
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

    stdout = result.stdout
    assert "FAKE_ACCESS_TOKEN_SHOULD_NOT_PRINT" not in stdout
    assert "FAKE_REFRESH_TOKEN_SHOULD_NOT_PRINT" not in stdout
    assert "PARTNER_KEY_SHOULD_NOT_PRINT" not in stdout


def test_ops_readiness_env_file_loaded_no_secret(tmp_path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path, include_shop_id=True)
    env_path = tmp_path / ".env.phase1.local"

    env_path.write_text(
        "\n".join(
            [
                "SHOPEE_PARTNER_KEY=PARTNER_KEY_SHOULD_NOT_PRINT",
                "ADS_DAILY_PATH=/api/v2/marketing/report/daily",
            ]
        ),
        encoding="utf-8",
    )

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env.pop("SHOPEE_PARTNER_KEY", None)
    env.pop("ADS_DAILY_PATH", None)
    env.pop("ADS_SNAPSHOT_PATH", None)

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "readiness",
        "phase1",
        "--shops",
        "samord,minmin",
        "--env-file",
        str(env_path),
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
    assert "env_file_loaded" in stdout
    assert "PARTNER_KEY_SHOULD_NOT_PRINT" not in stdout
