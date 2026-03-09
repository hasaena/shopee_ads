from __future__ import annotations

import json
import zipfile
from pathlib import Path
from types import SimpleNamespace

from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.discord_notifier import send


def _write_html(path: Path) -> None:
    path.write_text("<html><body>ok</body></html>", encoding="utf-8")


def _write_md(path: Path) -> None:
    path.write_text("# report\n\nok\n", encoding="utf-8")


def _write_zip(zip_path: Path, html_path: Path) -> None:
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.write(html_path, arcname=html_path.name)


def test_discord_send_with_html_attachment(monkeypatch, tmp_path: Path, capsys) -> None:
    html_path = tmp_path / "report.html"
    _write_html(html_path)

    monkeypatch.setenv("DISCORD_WEBHOOK_REPORT_URL", "https://discord.local/report")
    monkeypatch.delenv("DISCORD_DRY_RUN", raising=False)
    get_settings.cache_clear()

    calls: list[dict[str, object]] = []

    def fake_post(url: str, **kwargs):
        calls.append({"url": url, **kwargs})
        return SimpleNamespace(status_code=204, text="")

    monkeypatch.setattr("dotori_shopee_automation.discord_notifier.httpx.post", fake_post)

    send(
        "report",
        "Daily report",
        shop_label="SAMORD",
        attachment_path=html_path,
        attachment_filename="SAMORD_2026-02-25_midday.html",
    )
    output = capsys.readouterr().out

    assert len(calls) == 1
    assert calls[0]["url"] == "https://discord.local/report"
    assert "data" in calls[0]
    assert "files" in calls[0]
    payload_json = calls[0]["data"]["payload_json"]  # type: ignore[index]
    payload = json.loads(str(payload_json))
    assert payload["content"] == ""
    assert isinstance(payload.get("embeds"), list)
    file_payload = calls[0]["files"]["files[0]"]  # type: ignore[index]
    assert file_payload[0] == "SAMORD_2026-02-25_midday.html"
    assert file_payload[2] == "text/html; charset=utf-8"
    assert "report_attach_planned=1 channel=report shop_label=SAMORD" in output
    assert "discord_send_ok=1 channel=report shop_label=SAMORD http_status=204" in output
    assert "report_attach_sent=1 channel=report shop_label=SAMORD" in output
    get_settings.cache_clear()


def test_discord_send_with_html_zip_md_attachments(monkeypatch, tmp_path: Path, capsys) -> None:
    html_path = tmp_path / "report.html"
    zip_path = tmp_path / "report.zip"
    md_path = tmp_path / "report.md"
    _write_html(html_path)
    _write_zip(zip_path, html_path)
    _write_md(md_path)

    monkeypatch.setenv("DISCORD_WEBHOOK_REPORT_URL", "https://discord.local/report")
    monkeypatch.delenv("DISCORD_DRY_RUN", raising=False)
    get_settings.cache_clear()

    calls: list[dict[str, object]] = []

    def fake_post(url: str, **kwargs):
        calls.append({"url": url, **kwargs})
        return SimpleNamespace(status_code=204, text="")

    monkeypatch.setattr("dotori_shopee_automation.discord_notifier.httpx.post", fake_post)

    send(
        "report",
        "Daily report all attachments",
        shop_label="SAMORD",
        attachment_path=html_path,
        attachment_filename="SAMORD_2026-02-25_midday.html",
        zip_attachment_path=zip_path,
        zip_attachment_filename="SAMORD_2026-02-25_midday.zip",
        md_attachment_path=md_path,
        md_attachment_filename="SAMORD_2026-02-25_midday.md",
    )
    output = capsys.readouterr().out

    assert len(calls) == 1
    files_payload = calls[0]["files"]  # type: ignore[index]
    assert "files[0]" in files_payload
    assert "files[1]" in files_payload
    assert "files[2]" in files_payload
    assert files_payload["files[0]"][2] == "text/html; charset=utf-8"
    assert files_payload["files[1]"][2] == "application/zip"
    assert files_payload["files[2]"][2] == "text/markdown; charset=utf-8"
    assert "report_attach_sent=1 channel=report shop_label=SAMORD" in output
    assert "report_zip_attach_sent=1 channel=report shop_label=SAMORD" in output
    assert "report_md_attach_sent=1 channel=report shop_label=SAMORD" in output
    get_settings.cache_clear()


def test_discord_dry_run_with_attachment_does_not_post(monkeypatch, tmp_path: Path, capsys) -> None:
    html_path = tmp_path / "report.html"
    _write_html(html_path)

    monkeypatch.setenv("DISCORD_WEBHOOK_REPORT_URL", "https://discord.local/report")
    monkeypatch.setenv("DISCORD_DRY_RUN", "1")
    get_settings.cache_clear()

    calls = {"post": 0}

    def fake_post(url: str, **kwargs):  # noqa: ARG001
        calls["post"] += 1
        return SimpleNamespace(status_code=204, text="")

    monkeypatch.setattr("dotori_shopee_automation.discord_notifier.httpx.post", fake_post)

    send("report", "Dry run report", shop_label="MINMIN", attachment_path=html_path)
    output = capsys.readouterr().out

    assert calls["post"] == 0
    assert "report_attach_planned=1 channel=report shop_label=MINMIN" in output
    assert "discord_dry_run=1 channel=report shop_label=MINMIN" in output
    get_settings.cache_clear()
