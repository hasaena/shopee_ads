from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from typer.testing import CliRunner

import dotori_shopee_automation.ads.provider_live_plan as provider_live_plan
import dotori_shopee_automation.cli as cli_module
from dotori_shopee_automation.cli import app
from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.db import SessionLocal, init_db
from dotori_shopee_automation.shopee.token_store import upsert_token


runner = CliRunner()
REPO_ROOT = Path(__file__).resolve().parents[1]


def _write_shops(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "- shop_key: samord",
                "  label: SAMORD",
                "  enabled: true",
                "  shopee_shop_id: 111",
                "- shop_key: minmin",
                "  label: MINMIN",
                "  enabled: true",
                "  shopee_shop_id: 222",
            ]
        ),
        encoding="utf-8",
    )


def test_token_mode_passive_blocks_refresh(monkeypatch, tmp_path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)
    db_path = tmp_path / "passive.db"

    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("SHOPEE_PARTNER_ID", "999")
    monkeypatch.setenv("SHOPEE_PARTNER_KEY", "PARTNER_KEY_SHOULD_NOT_PRINT")
    monkeypatch.setenv("ADS_DAILY_PATH", "/api/v2/marketing/report/daily")
    monkeypatch.setenv("ADS_SNAPSHOT_PATH", "/api/v2/marketing/report/snapshot")
    get_settings.cache_clear()

    init_db()
    session = SessionLocal()
    try:
        expired = datetime.now(timezone.utc) - timedelta(hours=1)
        upsert_token(session, "samord", 111, "ACCESS", "REFRESH", expired)
        upsert_token(session, "minmin", 222, "ACCESS", "REFRESH", expired)
        session.commit()
    finally:
        session.close()

    def fail_refresh(*_args, **_kwargs):
        raise AssertionError("refresh_access_token should not be called in passive mode")

    monkeypatch.setattr(provider_live_plan, "refresh_access_token", fail_refresh)
    monkeypatch.setattr(cli_module, "_build_shopee_client", lambda settings: None)

    result = runner.invoke(
        app,
        [
            "ops",
            "phase1",
            "preview",
            "--date",
            "2026-02-03",
            "--only-shops",
            "samord,minmin",
            "--transport",
            "live",
            "--allow-network",
            "--token-mode",
            "passive",
            "--token-file",
            str(REPO_ROOT / "tests" / "fixtures" / "appsscript_tokens" / "shopee_tokens_export_example.json"),
            "--no-token-sync",
        ],
    )

    assert result.exit_code != 0
    assert "token_expired_refresh_disabled" in result.output
