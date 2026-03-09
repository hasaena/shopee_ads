from __future__ import annotations

import time
from typing import Any

import httpx

from .signing import build_sign_base, sign_hmac_sha256_hex


class ShopeeClient:
    def __init__(
        self,
        partner_id: int,
        partner_key: str,
        host: str,
        timeout: float = 10.0,
        transport: httpx.BaseTransport | None = None,
        max_retries: int = 2,
    ) -> None:
        self.partner_id = partner_id
        self.partner_key = partner_key
        self.host = host.rstrip("/")
        self.max_retries = max_retries
        self._client = httpx.Client(base_url=self.host, timeout=timeout, transport=transport)

    def request(
        self,
        method: str,
        path: str,
        *,
        shop_id: int | None = None,
        access_token: str | None = None,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        timestamp: int | None = None,
        omit_access_token_in_sign: bool = False,
    ) -> dict[str, Any]:
        ts = int(timestamp or time.time())
        sign_base = build_sign_base(
            self.partner_id,
            path,
            ts,
            access_token=access_token,
            shop_id=shop_id,
            omit_access_token=omit_access_token_in_sign,
        )
        sign = sign_hmac_sha256_hex(sign_base, self.partner_key)

        query: dict[str, Any] = {
            "partner_id": self.partner_id,
            "timestamp": ts,
            "sign": sign,
        }
        if shop_id is not None:
            query["shop_id"] = shop_id
        if access_token is not None:
            query["access_token"] = access_token
        if params:
            query.update(params)

        attempt = 0
        while True:
            try:
                response = self._client.request(method, path, params=query, json=json)
            except httpx.HTTPError:
                if attempt >= self.max_retries:
                    raise
                attempt += 1
                continue

            if response.status_code >= 500 and attempt < self.max_retries:
                attempt += 1
                continue

            response.raise_for_status()
            return response.json()

    def close(self) -> None:
        self._client.close()
