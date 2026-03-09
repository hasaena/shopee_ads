from __future__ import annotations

import httpx

from dotori_shopee_automation.shopee.client import ShopeeClient


def test_access_token_plus_is_percent_encoded() -> None:
    captured: dict[str, str] = {"raw_query": ""}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["raw_query"] = str(request.url).split("?", 1)[-1]
        return httpx.Response(200, json={"ok": True})

    client = ShopeeClient(
        partner_id=123,
        partner_key="TEST_PARTNER_KEY",
        host="https://example.com",
        transport=httpx.MockTransport(handler),
    )
    client.request(
        "GET",
        "/api/v2/shop/get_shop_info",
        shop_id=111,
        access_token="AA+BB/CC==",
        timestamp=1700000000,
    )

    raw_query = captured["raw_query"]
    assert "access_token=AA%2BBB%2FCC%3D%3D" in raw_query
    assert "access_token=AA+BB" not in raw_query
