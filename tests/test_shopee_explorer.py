from datetime import datetime, timedelta, timezone
from pathlib import Path
import json

import httpx
from typer.testing import CliRunner

import dotori_shopee_automation.cli as cli_module
from dotori_shopee_automation.cli import app
from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.db import SessionLocal, init_db
from dotori_shopee_automation.shopee.client import ShopeeClient
from dotori_shopee_automation.shopee.redact import redact_secrets
from dotori_shopee_automation.shopee.token_store import upsert_token


runner = CliRunner()


def _setup_env(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "shopee.db"
    shops_path = tmp_path / "shops.yaml"
    shops_path.write_text(
        "\n".join(
            [
                "- shop_key: shop_a",
                "  label: SHOP_A",
                "  enabled: true",
                "  shopee_shop_id: 123456",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    monkeypatch.setenv("SHOPEE_PARTNER_ID", "1000")
    monkeypatch.setenv("SHOPEE_PARTNER_KEY", "secret_key")
    monkeypatch.setenv("SHOPEE_API_HOST", "https://test.local")
    monkeypatch.setenv("SHOPEE_REDIRECT_URL", "https://example.com/callback")
    get_settings.cache_clear()
    init_db()


def test_redact_secrets_dict_and_list() -> None:
    payload = {
        "access_token": "ACCESS_TOKEN_SECRET",
        "refresh_token": "REFRESH_TOKEN_SECRET",
        "authToken": "SHOULD_HIDE",
        "nested": {"keep": "ok", "refresh_token": "INNER_REFRESH"},
        "items": [
            {"token": "INNER_TOKEN"},
            "token=INLINE_TOKEN",
            "https://example.com?access_token=URL_TOKEN&foo=1",
        ],
    }
    redacted = redact_secrets(payload)
    assert redacted["access_token"] == "***"
    assert redacted["refresh_token"] == "***"
    assert redacted["authToken"] == "***"
    assert redacted["nested"]["refresh_token"] == "***"
    assert redacted["nested"]["keep"] == "ok"
    assert redacted["items"][0]["token"] == "***"
    assert redacted["items"][1] == "token=***"
    assert "URL_TOKEN" not in redacted["items"][2]


def test_shopee_call_redacts_output_and_saves(monkeypatch, tmp_path) -> None:
    _setup_env(tmp_path, monkeypatch)
    monkeypatch.chdir(tmp_path)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v2/shop/get_shop_info":
            return httpx.Response(
                200,
                json={
                    "error": 0,
                    "access_token": "ACCESS_TOKEN_SECRET",
                    "refresh_token": "REFRESH_TOKEN_SECRET",
                    "response": {
                        "shop_id": 123456,
                        "shop_name": "Demo Shop",
                        "token": "INNER_TOKEN",
                    },
                },
            )
        return httpx.Response(404, json={"error": 1, "message": "not found"})

    transport = httpx.MockTransport(handler)
    client = ShopeeClient(1000, "secret_key", "https://test.local", transport=transport)
    monkeypatch.setattr(cli_module, "_build_shopee_client", lambda settings: client)

    with SessionLocal() as session:
        upsert_token(
            session,
            "shop_a",
            123456,
            "ACCESS_TOKEN_SECRET",
            "REFRESH_TOKEN_SECRET",
            datetime.now(timezone.utc) + timedelta(hours=1),
        )
        session.commit()

    result = runner.invoke(
        app,
        [
            "shopee",
            "call",
            "--shop",
            "shop_a",
            "--method",
            "GET",
            "--path",
            "/api/v2/shop/get_shop_info",
            "--save",
            "--pretty",
        ],
    )
    assert result.exit_code == 0
    assert "ACCESS_TOKEN_SECRET" not in result.output
    assert "REFRESH_TOKEN_SECRET" not in result.output

    saved_line = next(
        line for line in result.output.splitlines() if line.startswith("saved=")
    )
    saved_value = saved_line.split("=", 1)[1].strip()
    saved_path = Path(saved_value)
    if not saved_path.is_absolute():
        saved_path = tmp_path / saved_path
    assert saved_path.exists()

    saved_payload = json.loads(saved_path.read_text(encoding="utf-8"))
    assert saved_payload["access_token"] == "***"
    assert saved_payload["refresh_token"] == "***"
    assert saved_payload["response"]["token"] == "***"
