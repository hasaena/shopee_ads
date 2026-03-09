from __future__ import annotations

import zipfile
from pathlib import Path
from types import SimpleNamespace

from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.discord_notifier import send


def test_discord_zip_attachment_dry_run_no_network(monkeypatch, tmp_path: Path, capsys) -> None:
    html_path = tmp_path / "report.html"
    html_path.write_text("<html><body>ok</body></html>", encoding="utf-8")
    zip_path = tmp_path / "report.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.write(html_path, arcname=html_path.name)

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
        "Dry run with zip attachment",
        shop_label="SAMORD",
        attachment_path=html_path,
        zip_attachment_path=zip_path,
        zip_attachment_filename="SAMORD_2026-02-25_midday.zip",
    )
    output = capsys.readouterr().out

    assert calls["post"] == 0
    assert "report_attach_planned=1 channel=report shop_label=SAMORD" in output
    assert "report_zip_attach_planned=1 channel=report shop_label=SAMORD" in output
    assert "discord_dry_run=1 channel=report shop_label=SAMORD" in output
    get_settings.cache_clear()
