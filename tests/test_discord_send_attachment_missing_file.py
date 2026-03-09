from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.discord_notifier import send


def test_discord_send_attachment_missing_file_falls_back_to_message_only(
    monkeypatch, capsys
) -> None:
    missing_path = Path("D:/__not_exists__/missing_report.html")
    monkeypatch.setenv("DISCORD_WEBHOOK_REPORT_URL", "https://discord.local/report")
    monkeypatch.delenv("DISCORD_DRY_RUN", raising=False)
    get_settings.cache_clear()

    calls: list[dict[str, object]] = []

    def fake_post(url: str, **kwargs):
        calls.append({"url": url, **kwargs})
        return SimpleNamespace(status_code=204, text="")

    monkeypatch.setattr("dotori_shopee_automation.discord_notifier.httpx.post", fake_post)

    send("report", "Missing attachment", shop_label="MINMIN", attachment_path=missing_path)
    output = capsys.readouterr().out

    assert len(calls) == 1
    assert calls[0]["url"] == "https://discord.local/report"
    assert "json" in calls[0]
    assert "files" not in calls[0]
    assert (
        "report_attach_skipped=1 reason=file_missing channel=report shop_label=MINMIN"
        in output
    )
    assert "discord_send_ok=1 channel=report shop_label=MINMIN http_status=204" in output
    get_settings.cache_clear()
