from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from typer.testing import CliRunner

import dotori_shopee_automation.cli as cli_module
from dotori_shopee_automation.cli import app
from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.db import SessionLocal, init_db
from dotori_shopee_automation.shopee.client import ShopeeClient
from dotori_shopee_automation.shopee.token_store import upsert_token

runner = CliRunner()


def _setup_env(tmp_path, monkeypatch) -> Path:
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
    return db_path


def test_exchange_code_and_no_secret_leak(monkeypatch, tmp_path) -> None:
    _setup_env(tmp_path, monkeypatch)
    calls = {"token_get": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v2/auth/token/get":
            calls["token_get"] += 1
            return httpx.Response(
                200,
                json={
                    "error": 0,
                    "access_token": "ACCESS_TOKEN_SECRET",
                    "refresh_token": "REFRESH_TOKEN_SECRET",
                    "expire_in": 3600,
                    "shop_id": 123456,
                },
            )
        return httpx.Response(404, json={"error": 1, "message": "not found"})

    transport = httpx.MockTransport(handler)
    client = ShopeeClient(1000, "secret_key", "https://test.local", transport=transport)
    monkeypatch.setattr(cli_module, "_build_shopee_client", lambda settings: client)

    result = runner.invoke(
        app,
        [
            "shopee",
            "exchange-code",
            "--shop",
            "shop_a",
            "--code",
            "dummy",
            "--timestamp",
            "1700000000",
        ],
    )
    assert result.exit_code == 0
    assert "ACCESS_TOKEN_SECRET" not in result.output
    assert "REFRESH_TOKEN_SECRET" not in result.output
    assert "shop_key=shop_a" in result.output
    assert calls["token_get"] == 1


def test_ping_triggers_refresh(monkeypatch, tmp_path) -> None:
    _setup_env(tmp_path, monkeypatch)
    calls = {"refresh": 0, "ping": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v2/auth/access_token/get":
            calls["refresh"] += 1
            return httpx.Response(
                200,
                json={
                    "error": 0,
                    "access_token": "NEW_ACCESS",
                    "refresh_token": "NEW_REFRESH",
                    "expire_in": 3600,
                    "shop_id": 123456,
                },
            )
        if request.url.path == "/api/v2/shop/get_shop_info":
            calls["ping"] += 1
            return httpx.Response(
                200,
                json={
                    "error": 0,
                    "response": {"shop_id": 123456, "shop_name": "Demo Shop"},
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
            "OLD_ACCESS",
            "OLD_REFRESH",
            datetime.now(timezone.utc) - timedelta(seconds=10),
        )
        session.commit()

    result = runner.invoke(app, ["shopee", "ping", "--shop", "shop_a"])
    assert result.exit_code == 0
    assert "shop_name=Demo Shop" in result.output
    assert calls["refresh"] == 1
    assert calls["ping"] == 1


def test_refresh_token_accepts_empty_string_error(monkeypatch, tmp_path) -> None:
    _setup_env(tmp_path, monkeypatch)
    calls = {"refresh": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v2/auth/access_token/get":
            calls["refresh"] += 1
            return httpx.Response(
                200,
                json={
                    "error": "",
                    "message": "",
                    "access_token": "NEW_ACCESS",
                    "refresh_token": "NEW_REFRESH",
                    "expire_in": 3600,
                    "shop_id": 123456,
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
            "OLD_ACCESS",
            "OLD_REFRESH",
            datetime.now(timezone.utc) - timedelta(seconds=10),
        )
        session.commit()

    result = runner.invoke(
        app,
        [
            "shopee",
            "refresh-token",
            "--shop",
            "shop_a",
            "--timestamp",
            "1700000000",
        ],
    )
    assert result.exit_code == 0
    assert "shop_key=shop_a shop_id=123456 access_expires_at=" in result.output
    assert calls["refresh"] == 1
