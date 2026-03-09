from __future__ import annotations

from typing import Any

from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.discord_notifier import send


def test_alert_send_uses_embed_and_keeps_text_content(monkeypatch) -> None:
    monkeypatch.setenv("DISCORD_WEBHOOK_ALERTS_URL", "https://discord.example/alerts")
    get_settings.cache_clear()

    calls: list[dict[str, Any]] = []

    def fake_post(url: str, json=None, timeout=None):  # noqa: A002
        calls.append({"url": url, "json": json, "timeout": timeout})

        class _Resp:
            status_code = 204
            text = ""

        return _Resp()

    monkeypatch.setattr("dotori_shopee_automation.discord_notifier.httpx.post", fake_post)

    message = "\n".join(
        [
            "CẢNH BÁO CRITICAL - Token push thất bại",
            "Sự kiện: TOKEN_PUSH_FAILED",
            "Lỗi: token_import_failed",
            "Hành động: kiểm tra server logs.",
        ]
    )
    send("alerts", message, shop_label="OPS")

    assert len(calls) == 1
    payload = calls[0]["json"] or {}
    assert payload.get("content", "") in {"", "[OPS][ALERT] CẢNH BÁO CRITICAL - Token push thất bại"}
    embeds = payload.get("embeds")
    assert isinstance(embeds, list) and len(embeds) == 1
    embed = embeds[0]
    assert "[OPS]" in str(embed.get("title") or "")
    assert "CRITICAL" in str(embed.get("description") or "")
