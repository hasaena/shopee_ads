from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.discord_notifier import send


def test_discord_md_attachment_dry_run_no_network(monkeypatch, tmp_path: Path, capsys) -> None:
    md_path = tmp_path / "report.md"
    md_path.write_text("# report\n\nok\n", encoding="utf-8")

    monkeypatch.setenv("DISCORD_WEBHOOK_REPORT_URL", "https://discord.local/report")
    monkeypatch.setenv("DISCORD_DRY_RUN", "1")
    get_settings.cache_clear()

    calls = {"post": 0}

    def fake_post(url: str, **kwargs):  # noqa: ARG001
        calls["post"] += 1
        return SimpleNamespace(status_code=204, text="")

    monkeypatch.setattr("dotori_shopee_automation.discord_notifier.httpx.post", fake_post)

    send(
        "report",
        "Dry run with md attachment",
        shop_label="MINMIN",
        md_attachment_path=md_path,
        md_attachment_filename="MINMIN_2026-02-25_midday.md",
    )
    output = capsys.readouterr().out

    assert calls["post"] == 0
    assert "report_md_attach_planned=1 channel=report shop_label=MINMIN" in output
    assert "discord_dry_run=1 channel=report shop_label=MINMIN" in output
    get_settings.cache_clear()
