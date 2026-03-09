from __future__ import annotations

import httpx

from dotori_shopee_automation.shopee.client import ShopeeClient


def test_outgoing_access_token_not_redacted_in_transport() -> None:
    captured: dict[str, str | None] = {"access_token": None}

    def handler(request: httpx.Request) -> httpx.Response:
        params = dict(request.url.params)
        captured["access_token"] = params.get("access_token")
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
        access_token="REAL_TOKEN_VALUE",
        timestamp=1700000000,
    )

    assert captured["access_token"] == "REAL_TOKEN_VALUE"
