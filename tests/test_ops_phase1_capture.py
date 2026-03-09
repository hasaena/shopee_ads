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
PLAN_PATH = REPO_ROOT / "collaboration" / "plans" / "ads_ingest_minimal.yaml"
MAPPING_PATH = REPO_ROOT / "collaboration" / "mappings" / "ads_mapping.yaml"
TOKEN_FIXTURE = (
    REPO_ROOT
    / "tests"
    / "fixtures"
    / "appsscript_tokens"
    / "shopee_tokens_export_raw_properties_example.json"
)


def _write_shops(path: Path) -> None:
    lines = [
        "- shop_key: samord",
        "  label: SAMORD",
        "  enabled: true",
        "  shopee_shop_id: 111",
        "- shop_key: minmin",
        "  label: MINMIN",
        "  enabled: true",
        "  shopee_shop_id: 222",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def _seed_tokens(db_url: str, access: str, refresh: str) -> None:
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


def test_ops_phase1_capture_fixtures(tmp_path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)
    db_path = tmp_path / "capture.db"
    db_url = f"sqlite:///{db_path}"
    reports_dir = tmp_path / "reports"
    out_md = tmp_path / "capture.md"
    env_path = tmp_path / ".env.phase1.local"
    env_path.write_text(
        "\n".join(
            [
                "SHOPEE_SAMORD_SHOP_ID=497412318",
                "SHOPEE_MINMIN_SHOP_ID=567655304",
            ]
        ),
        encoding="utf-8",
    )

    _seed_tokens(
        db_url,
        access="ACCESS_TOKEN_SHOULD_NOT_PRINT",
        refresh="REFRESH_TOKEN_SHOULD_NOT_PRINT",
    )

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
        "capture",
        "--date",
        "2026-02-03",
        "--only-shops",
        "samord,minmin",
        "--transport",
        "fixtures",
        "--fixtures-dir",
        str(FIXTURE_ROOT),
        "--plan",
        str(PLAN_PATH),
        "--mapping",
        str(MAPPING_PATH),
        "--reports-dir",
        str(reports_dir),
        "--out-md",
        str(out_md),
        "--env-file",
        str(env_path),
        "--token-file",
        str(TOKEN_FIXTURE),
        "--no-send-discord",
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
    assert "capture_md path=" in result.stdout
    content = out_md.read_text(encoding="utf-8")
    assert "phase1_verify" in content
    assert "token_appsscript_import_start" in content
    assert "phase1_preview_start" in content
    assert "report_path shop=samord" in content
    assert "report_path shop=minmin" in content


def test_ops_phase1_capture_live_gate(tmp_path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)
    db_path = tmp_path / "capture_live.db"
    out_md = tmp_path / "capture_live.md"
    env_path = tmp_path / ".env.phase1.local"
    env_path.write_text("# empty env\n", encoding="utf-8")

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env["DATABASE_URL"] = f"sqlite:///{db_path}"

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "capture",
        "--date",
        "2026-02-03",
        "--only-shops",
        "samord,minmin",
        "--transport",
        "live",
        "--out-md",
        str(out_md),
        "--env-file",
        str(env_path),
        "--no-send-discord",
        "--no-token-db-auto",
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
    content = out_md.read_text(encoding="utf-8")
    assert "network_disabled error=allow_network_required" in content


def test_ops_phase1_capture_no_secrets(tmp_path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)
    db_path = tmp_path / "capture_secret.db"
    db_url = f"sqlite:///{db_path}"
    out_md = tmp_path / "capture_secret.md"
    env_path = tmp_path / ".env.phase1.local"
    env_path.write_text("# empty env\n", encoding="utf-8")

    _seed_tokens(
        db_url,
        access="ACCESS_TOKEN_SHOULD_NOT_PRINT",
        refresh="REFRESH_TOKEN_SHOULD_NOT_PRINT",
    )

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
        "capture",
        "--date",
        "2026-02-03",
        "--only-shops",
        "samord,minmin",
        "--transport",
        "fixtures",
        "--fixtures-dir",
        str(FIXTURE_ROOT),
        "--plan",
        str(PLAN_PATH),
        "--mapping",
        str(MAPPING_PATH),
        "--out-md",
        str(out_md),
        "--env-file",
        str(env_path),
        "--no-send-discord",
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
    content = out_md.read_text(encoding="utf-8")
    assert "PARTNER_KEY_SHOULD_NOT_PRINT" not in content
    assert "ACCESS_TOKEN_SHOULD_NOT_PRINT" not in content
    assert "REFRESH_TOKEN_SHOULD_NOT_PRINT" not in content
