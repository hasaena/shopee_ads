from typer.testing import CliRunner

import dotori_shopee_automation.cli as cli_module
from dotori_shopee_automation.cli import app
from dotori_shopee_automation.config import get_settings

runner = CliRunner()


def test_health_cli() -> None:
    result = runner.invoke(app, ["health"])
    assert result.exit_code == 0
    assert "ok" in result.output


def test_discord_test_warns_without_url(monkeypatch) -> None:
    monkeypatch.delenv("DISCORD_WEBHOOK_REPORT_URL", raising=False)
    get_settings.cache_clear()
    result = runner.invoke(
        app,
        ["discord-test", "--channel", "report", "--text", "hello"],
    )
    assert result.exit_code == 0
    assert "Discord webhook URL not set" in result.output


def test_discord_test_with_shop_prefix(monkeypatch, tmp_path) -> None:
    shops_path = tmp_path / "shops.yaml"
    shops_path.write_text(
        "\n".join(
            [
                "- shop_key: shop_a",
                "  label: SHOP_A",
                "  enabled: true",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    get_settings.cache_clear()

    captured: dict[str, str | None] = {}

    def fake_send(channel, text, shop_label=None, webhook_url=None) -> None:
        captured["channel"] = channel
        captured["text"] = text
        captured["shop_label"] = shop_label
        captured["webhook_url"] = webhook_url

    monkeypatch.setattr(cli_module, "send", fake_send)

    result = runner.invoke(
        app,
        ["discord-test", "--channel", "report", "--text", "hello", "--shop", "shop_a"],
    )
    assert result.exit_code == 0
    assert captured["shop_label"] == "SHOP_A"
