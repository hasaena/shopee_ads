from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from typer.testing import CliRunner

import dotori_shopee_automation.cli as cli_module
from dotori_shopee_automation.cli import app
from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.db import SessionLocal, init_db
from dotori_shopee_automation.shopee.client import ShopeeClient
from dotori_shopee_automation.shopee.token_store import upsert_token
import dotori_shopee_automation.shopee.probe_suite as probe_suite_module


runner = CliRunner()


def _setup_env(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "shopee.db"
    shops_path = tmp_path / "shops.yaml"
    shops_path.write_text(
        "\n".join(
            [
                "- shop_key: shop_a",
                "  label: SHOP_A",
                "  enabled: true",
                "  shopee_shop_id: 123456",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    monkeypatch.setenv("SHOPEE_PARTNER_ID", "1000")
    monkeypatch.setenv("SHOPEE_PARTNER_KEY", "secret_key")
    monkeypatch.setenv("SHOPEE_API_HOST", "https://test.local")
    monkeypatch.setenv("SHOPEE_REDIRECT_URL", "https://example.com/callback")
    get_settings.cache_clear()
    init_db()


def test_probe_suite_happy_path(monkeypatch, tmp_path) -> None:
    _setup_env(tmp_path, monkeypatch)
    monkeypatch.chdir(tmp_path)

    plan_path = tmp_path / "plan.yaml"
    plan_path.write_text(
        "\n".join(
            [
                "version: 1",
                "name: probe",
                "calls:",
                "  - name: shop_info",
                "    path: /api/v2/shop/get_shop_info",
                "    params: {}",
            ]
        ),
        encoding="utf-8",
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"error": 0, "response": {"shop_id": 123456, "shop_name": "Demo"}},
        )

    transport = httpx.MockTransport(handler)
    client = ShopeeClient(1000, "secret_key", "https://test.local", transport=transport)
    monkeypatch.setattr(cli_module, "_build_shopee_client", lambda settings: client)

    with SessionLocal() as session:
        upsert_token(
            session,
            "shop_a",
            123456,
            "ACCESS_TOKEN_SECRET",
            "REFRESH_TOKEN_SECRET",
            datetime.now(timezone.utc) + timedelta(hours=1),
        )
        session.commit()

    result = runner.invoke(
        app,
        [
            "shopee",
            "probe-suite",
            "--date",
            "20260203",
            "--only-shops",
            "shop_a",
            "--plan",
            str(plan_path),
        ],
    )
    assert result.exit_code == 0
    assert "plan_summary" in result.output
    assert "analyze_outputs" in result.output

    out_dir = (
        tmp_path
        / "collaboration"
        / "outputs"
        / "probe_summaries"
        / "20260203"
    )
    assert (out_dir / "probe_summary_20260203.md").exists()
    assert (out_dir / "probe_summary_20260203.csv").exists()


def test_probe_suite_dry_run_no_artifacts(monkeypatch, tmp_path) -> None:
    _setup_env(tmp_path, monkeypatch)
    monkeypatch.chdir(tmp_path)

    plan_path = tmp_path / "plan.yaml"
    plan_path.write_text(
        "\n".join(
            [
                "version: 1",
                "name: probe",
                "calls:",
                "  - name: shop_info",
                "    path: /api/v2/shop/get_shop_info",
                "    params: {}",
            ]
        ),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "shopee",
            "probe-suite",
            "--date",
            "20260203",
            "--only-shops",
            "shop_a",
            "--plan",
            str(plan_path),
            "--dry-run",
        ],
    )
    assert result.exit_code == 0
    assert "analyze=no_artifacts" in result.output


def test_probe_suite_send_discord(monkeypatch, tmp_path) -> None:
    _setup_env(tmp_path, monkeypatch)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DISCORD_WEBHOOK_REPORT_URL", "https://discord.local/webhook")
    get_settings.cache_clear()

    plan_path = tmp_path / "plan.yaml"
    plan_path.write_text(
        "\n".join(
            [
                "version: 1",
                "name: probe",
                "calls:",
                "  - name: shop_info",
                "    path: /api/v2/shop/get_shop_info",
                "    params: {}",
            ]
        ),
        encoding="utf-8",
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"error": 0, "response": {"shop_id": 1}})

    transport = httpx.MockTransport(handler)
    client = ShopeeClient(1000, "secret_key", "https://test.local", transport=transport)
    monkeypatch.setattr(cli_module, "_build_shopee_client", lambda settings: client)

    with SessionLocal() as session:
        upsert_token(
            session,
            "shop_a",
            123456,
            "ACCESS_TOKEN_SECRET",
            "REFRESH_TOKEN_SECRET",
            datetime.now(timezone.utc) + timedelta(hours=1),
        )
        session.commit()

    sent = {}

    def fake_send(channel, text, shop_label=None, webhook_url=None):
        sent["channel"] = channel
        sent["text"] = text

    monkeypatch.setattr(probe_suite_module, "send", fake_send)

    result = runner.invoke(
        app,
        [
            "shopee",
            "probe-suite",
            "--date",
            "20260203",
            "--only-shops",
            "shop_a",
            "--plan",
            str(plan_path),
            "--send-discord",
            "--channel",
            "report",
        ],
    )
    assert result.exit_code == 0
    assert sent["channel"] == "report"
    assert "PROBE" in sent["text"]
