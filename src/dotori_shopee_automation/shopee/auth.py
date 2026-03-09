from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode

from .client import ShopeeClient
from .signing import build_sign_base, sign_hmac_sha256_hex


@dataclass(frozen=True)
class TokenResponse:
    access_token: str
    refresh_token: str
    expires_in: int
    shop_id: int

    @property
    def access_expires_at(self) -> datetime:
        return datetime.now(timezone.utc) + timedelta(seconds=self.expires_in)


def build_auth_partner_url(
    partner_id: int,
    partner_key: str,
    redirect_url: str,
    timestamp: int,
    host: str,
) -> str:
    path = "/api/v2/shop/auth_partner"
    base = build_sign_base(partner_id, path, timestamp, omit_access_token=True)
    sign = sign_hmac_sha256_hex(base, partner_key)
    query = urlencode(
        {
            "partner_id": partner_id,
            "redirect": redirect_url,
            "timestamp": timestamp,
            "sign": sign,
        }
    )
    host = host.rstrip("/")
    return f"{host}{path}?{query}"


def exchange_code_for_token(
    client: ShopeeClient,
    partner_id: int,
    partner_key: str,
    shop_id: int,
    code: str,
    timestamp: int,
) -> TokenResponse:
    response = client.request(
        "POST",
        "/api/v2/auth/token/get",
        shop_id=shop_id,
        json={"code": code, "shop_id": shop_id, "partner_id": partner_id},
        timestamp=timestamp,
        omit_access_token_in_sign=True,
    )
    return _parse_token_response(response)


def refresh_access_token(
    client: ShopeeClient,
    partner_id: int,
    partner_key: str,
    shop_id: int,
    refresh_token: str,
    timestamp: int,
) -> TokenResponse:
    response = client.request(
        "POST",
        "/api/v2/auth/access_token/get",
        shop_id=shop_id,
        json={"refresh_token": refresh_token, "shop_id": shop_id, "partner_id": partner_id},
        timestamp=timestamp,
        omit_access_token_in_sign=True,
    )
    return _parse_token_response(response)


def _parse_token_response(payload: dict) -> TokenResponse:
    _ensure_success(payload)
    return TokenResponse(
        access_token=payload.get("access_token", ""),
        refresh_token=payload.get("refresh_token", ""),
        expires_in=int(payload.get("expire_in", 0)),
        shop_id=int(payload.get("shop_id", 0)),
    )


def _ensure_success(payload: dict) -> None:
    error = payload.get("error")
    if error in (None, "", 0, "0"):
        return
    if isinstance(error, str):
        if error.strip() in {"", "0"}:
            return
    if isinstance(error, (int, float)) and int(error) == 0:
        return
    if isinstance(error, bool) and not error:
        return
    if str(error).strip().lower() in {"none", "null", "false"}:
        return
    if error not in (0, None):
        message = payload.get("message", "API error")
        raise ValueError(f"Shopee API error {error}: {message}")
