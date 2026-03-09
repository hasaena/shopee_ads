from pathlib import Path
from typer.testing import CliRunner

import dotori_shopee_automation.cli as cli_module
from dotori_shopee_automation.cli import app
from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.db import init_db


runner = CliRunner()


def _setup_env(tmp_path, monkeypatch) -> Path:
    db_path = tmp_path / "smoke.db"
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
    get_settings.cache_clear()
    init_db()
    return shops_path


def test_ops_smoke_dry_run(monkeypatch, tmp_path) -> None:
    _setup_env(tmp_path, monkeypatch)
    monkeypatch.chdir(tmp_path)

    plan_path = tmp_path / "plan.yaml"
    plan_path.write_text(
        "\n".join(
            [
                "version: 1",
                "name: smoke",
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
            "ops",
            "smoke",
            "--only-shops",
            "shop_a",
            "--plan",
            str(plan_path),
            "--date",
            "20260203",
            "--no-send-discord",
            "--no-live-http",
        ],
    )
    assert result.exit_code == 0
    assert "smoke_start" in result.output
    assert "analyze=no_artifacts" in result.output


def test_ops_smoke_send_discord(monkeypatch, tmp_path) -> None:
    _setup_env(tmp_path, monkeypatch)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DISCORD_WEBHOOK_REPORT_URL", "https://discord.local/report")
    monkeypatch.setenv("DISCORD_WEBHOOK_ALERTS_URL", "https://discord.local/alerts")
    get_settings.cache_clear()

    plan_path = tmp_path / "plan.yaml"
    plan_path.write_text(
        "\n".join(
            [
                "version: 1",
                "name: smoke",
                "calls:",
                "  - name: shop_info",
                "    path: /api/v2/shop/get_shop_info",
                "    params: {}",
            ]
        ),
        encoding="utf-8",
    )

    sent_channels: list[str] = []

    def fake_send(channel, text, shop_label=None, webhook_url=None):
        sent_channels.append(channel)

    def fake_probe_suite(**kwargs):
        return {"ok": 1, "fail": 0, "md_path": "md", "csv_path": "csv"}

    monkeypatch.setattr(cli_module, "send", fake_send)
    monkeypatch.setattr(cli_module, "run_probe_suite", fake_probe_suite)

    result = runner.invoke(
        app,
        [
            "ops",
            "smoke",
            "--only-shops",
            "shop_a",
            "--plan",
            str(plan_path),
            "--date",
            "20260203",
            "--send-discord",
            "--no-live-http",
        ],
    )
    assert result.exit_code == 0
    assert "report" in sent_channels
    assert "alerts" in sent_channels


def test_ops_smoke_live_http_flag(monkeypatch, tmp_path) -> None:
    _setup_env(tmp_path, monkeypatch)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SHOPEE_PARTNER_ID", "1000")
    monkeypatch.setenv("SHOPEE_PARTNER_KEY", "secret_key")
    get_settings.cache_clear()

    plan_path = tmp_path / "plan.yaml"
    plan_path.write_text(
        "\n".join(
            [
                "version: 1",
                "name: smoke",
                "calls:",
                "  - name: shop_info",
                "    path: /api/v2/shop/get_shop_info",
                "    params: {}",
            ]
        ),
        encoding="utf-8",
    )

    captured = {}

    def fake_probe_suite(**kwargs):
        captured["dry_run"] = kwargs.get("dry_run")
        return {"ok": 0, "fail": 0}

    monkeypatch.setattr(cli_module, "run_probe_suite", fake_probe_suite)

    result = runner.invoke(
        app,
        [
            "ops",
            "smoke",
            "--only-shops",
            "shop_a",
            "--plan",
            str(plan_path),
            "--date",
            "20260203",
            "--live-http",
            "--no-send-discord",
        ],
    )
    assert result.exit_code == 0
    assert captured["dry_run"] is False
from pathlib import Path
