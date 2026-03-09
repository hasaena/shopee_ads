from __future__ import annotations

import hmac
import hashlib


def build_sign_base(
    partner_id: int,
    path: str,
    timestamp: int,
    access_token: str | None = None,
    shop_id: int | None = None,
    omit_access_token: bool = False,
) -> str:
    base = f"{partner_id}{path}{timestamp}"
    if access_token and not omit_access_token:
        base += access_token
    if shop_id is not None:
        base += str(shop_id)
    return base


def sign_hmac_sha256_hex(base: str, partner_key: str) -> str:
    return hmac.new(partner_key.encode("utf-8"), base.encode("utf-8"), hashlib.sha256).hexdigest()
