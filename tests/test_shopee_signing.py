from dotori_shopee_automation.shopee.signing import build_sign_base, sign_hmac_sha256_hex


def test_build_sign_base() -> None:
    base = build_sign_base(
        partner_id=123,
        path="/api/v2/shop/get_shop_info",
        timestamp=1700000000,
        access_token="token",
        shop_id=456,
    )
    assert base == "123/api/v2/shop/get_shop_info1700000000token456"

    base_no_token = build_sign_base(
        partner_id=123,
        path="/api/v2/auth/token/get",
        timestamp=1700000000,
        shop_id=456,
        omit_access_token=True,
    )
    assert base_no_token == "123/api/v2/auth/token/get1700000000456"


def test_sign_hmac_sha256_hex_length() -> None:
    signature = sign_hmac_sha256_hex("base", "secret")
    assert len(signature) == 64
