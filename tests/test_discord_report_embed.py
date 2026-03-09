from __future__ import annotations

from typing import Any

from dotori_shopee_automation.discord_notifier import send


def test_report_send_uses_embed_with_multiline_message(monkeypatch) -> None:
    monkeypatch.setenv("DISCORD_WEBHOOK_REPORT_URL", "https://discord.example/webhook")

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
            "Báo cáo Ads final 2026-03-01: spend=VND 220,000, gmv=VND 1,313,000, ROAS=5.97",
            "KPI: Chi tiêu=VND 220,000 | GMV=VND 1,313,000 | ROAS=5.97 | Đơn hàng=3",
            "Nguồn dữ liệu: daily_total_source=ads_daily campaign_breakdown_status=supported",
            "http://139.180.136.111/reports/minmin/daily/2026-03-01_final.html",
        ]
    )
    send("report", message, shop_label="MINMIN")

    assert len(calls) == 1
    payload = calls[0]["json"] or {}
    assert payload["content"] == ""
    embeds = payload.get("embeds")
    assert isinstance(embeds, list) and len(embeds) == 1
    embed = embeds[0]
    assert embed.get("title") == "[MINMIN] 260301_Daily Ads report"
    assert embed.get("url") == "http://139.180.136.111/reports/minmin/daily/2026-03-01_final.html"
    field_names = [str(field.get("name") or "") for field in embed.get("fields") or []]
    assert "KPI" in field_names
    assert "Liên kết" in field_names
    assert "Nguồn dữ liệu" not in field_names


def test_report_embed_extracts_inline_url(monkeypatch) -> None:
    monkeypatch.setenv("DISCORD_WEBHOOK_REPORT_URL", "https://discord.example/webhook")

    calls: list[dict[str, Any]] = []

    def fake_post(url: str, json=None, timeout=None):  # noqa: A002
        calls.append({"url": url, "json": json, "timeout": timeout})

        class _Resp:
            status_code = 204
            text = ""

        return _Resp()

    monkeypatch.setattr("dotori_shopee_automation.discord_notifier.httpx.post", fake_post)

    message = (
        "Báo cáo Ads weekly 2026-W09: spend=VND 1,000,000 | "
        "http://139.180.136.111/reports/minmin/weekly/2026-W09.html"
    )
    send("report", message, shop_label="MINMIN")

    payload = calls[0]["json"] or {}
    embed = (payload.get("embeds") or [])[0]
    assert embed.get("title") == "[MINMIN] 2026-W09_Weekly Ads report"
    assert embed.get("url") == "http://139.180.136.111/reports/minmin/weekly/2026-W09.html"
