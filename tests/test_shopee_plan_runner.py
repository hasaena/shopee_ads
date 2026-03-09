from datetime import datetime, timedelta, timezone
from pathlib import Path
import json

import httpx
import pytest
from typer.testing import CliRunner

import dotori_shopee_automation.cli as cli_module
from dotori_shopee_automation.cli import app
from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.db import SessionLocal, init_db
from dotori_shopee_automation.shopee.client import ShopeeClient
from dotori_shopee_automation.shopee.plan import (
    build_artifact_path,
    interpolate_data,
    load_plan,
    safe_path,
)
from dotori_shopee_automation.shopee.token_store import upsert_token


runner = CliRunner()


def _setup_env(tmp_path, monkeypatch, with_two_shops: bool = False) -> None:
    db_path = tmp_path / "shopee.db"
    shops_path = tmp_path / "shops.yaml"
    shops = [
        "- shop_key: shop_a",
        "  label: SHOP_A",
        "  enabled: true",
        "  shopee_shop_id: 123456",
    ]
    if with_two_shops:
        shops += [
            "- shop_key: shop_b",
            "  label: SHOP_B",
            "  enabled: true",
            "  shopee_shop_id: 654321",
        ]
    shops_path.write_text("\n".join(shops), encoding="utf-8")
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    monkeypatch.setenv("SHOPEE_PARTNER_ID", "1000")
    monkeypatch.setenv("SHOPEE_PARTNER_KEY", "secret_key")
    monkeypatch.setenv("SHOPEE_API_HOST", "https://test.local")
    monkeypatch.setenv("SHOPEE_REDIRECT_URL", "https://example.com/callback")
    get_settings.cache_clear()
    init_db()


def test_plan_parsing_and_interpolation(tmp_path) -> None:
    plan_path = tmp_path / "plan.yaml"
    plan_path.write_text(
        "\n".join(
            [
                "version: 1",
                "name: ads_probe",
                "defaults:",
                "  method: GET",
                "  save: true",
                "calls:",
                "  - name: shop_info",
                "    path: /api/v2/shop/get_shop_info",
                "    params:",
                "      date: \"{{date}}\"",
            ]
        ),
        encoding="utf-8",
    )
    plan_def = load_plan(plan_path)
    assert plan_def.name == "ads_probe"
    assert plan_def.calls[0].method == "GET"

    rendered = interpolate_data(plan_def.calls[0].params, {"date": "2026-02-01"})
    assert rendered["date"] == "2026-02-01"

    with pytest.raises(ValueError):
        interpolate_data(plan_def.calls[0].params, {})


def test_safe_path_and_artifact_path(tmp_path) -> None:
    assert safe_path("/api/v2/shop/get_shop_info") == "api_v2_shop_get_shop_info"
    when = datetime(2026, 2, 1, tzinfo=timezone.utc)
    root = tmp_path / "artifacts"
    path = build_artifact_path(root, "shop_a", "call", "/api/v2/shop/get_shop_info", when)
    ts_ms = int(when.timestamp() * 1000)
    expected = root / "shop_a" / "20260201" / f"{ts_ms}_call_api_v2_shop_get_shop_info.json"
    assert path == expected


def test_run_plan_continue_on_error_and_redaction(monkeypatch, tmp_path) -> None:
    _setup_env(tmp_path, monkeypatch)
    monkeypatch.chdir(tmp_path)

    plan_path = tmp_path / "plan.yaml"
    plan_path.write_text(
        "\n".join(
            [
                "version: 1",
                "name: ads_probe",
                "defaults:",
                "  method: GET",
                "  save: true",
                "calls:",
                "  - name: shop_info",
                "    path: /api/v2/shop/get_shop_info",
                "    params:",
                "      date: \"{{date}}\"",
                "      partner_key: SHOULD_HIDE",
                "      authorization: SHOULD_HIDE",
                "      sign: SHOULD_HIDE",
                "      client_secret: SHOULD_HIDE",
                "  - name: ads_fail",
                "    path: /api/v2/marketing/fail",
                "    params: {}",
            ]
        ),
        encoding="utf-8",
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v2/shop/get_shop_info":
            return httpx.Response(
                200,
                json={
                    "error": 0,
                    "access_token": "ACCESS_TOKEN_SECRET",
                    "refresh_token": "REFRESH_TOKEN_SECRET",
                    "response": {"shop_id": 123456, "shop_name": "Demo Shop"},
                },
            )
        if request.url.path == "/api/v2/marketing/fail":
            return httpx.Response(
                200,
                json={"error": 1, "message": "denied"},
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
            "run-plan",
            "--shop",
            "shop_a",
            "--plan",
            str(plan_path),
            "--vars",
            "date=2026-02-01",
            "--save-root",
            str(tmp_path / "artifacts"),
            "--no-print",
            "--continue-on-error",
        ],
    )
    assert result.exit_code == 0
    assert "call=shop_info" in result.output
    assert "call=ads_fail" in result.output

    saved_lines = [
        line for line in result.output.splitlines() if "call=shop_info" in line
    ]
    saved_value = saved_lines[0].split("saved=", 1)[1].split(" ", 1)[0]
    saved_path = Path(saved_value)
    if not saved_path.is_absolute():
        saved_path = tmp_path / saved_path
    payload = json.loads(saved_path.read_text(encoding="utf-8"))
    assert payload["access_token"] == "***"
    assert payload["refresh_token"] == "***"
    assert payload["__meta"]["params"]["partner_key"] == "***"
    assert payload["__meta"]["params"]["authorization"] == "***"
    assert payload["__meta"]["params"]["sign"] == "***"
    assert payload["__meta"]["params"]["client_secret"] == "***"
    assert payload["__meta"]["params"]["date"] == "2026-02-01"


def test_run_plan_all(monkeypatch, tmp_path) -> None:
    _setup_env(tmp_path, monkeypatch, with_two_shops=True)
    monkeypatch.chdir(tmp_path)

    plan_path = tmp_path / "plan.yaml"
    plan_path.write_text(
        "\n".join(
            [
                "version: 1",
                "name: ads_probe",
                "calls:",
                "  - name: shop_info",
                "    path: /api/v2/shop/get_shop_info",
                "    params: {}",
            ]
        ),
        encoding="utf-8",
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "error": 0,
                "response": {"shop_id": 123456, "shop_name": "Demo Shop"},
            },
        )

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
        upsert_token(
            session,
            "shop_b",
            654321,
            "ACCESS_TOKEN_SECRET",
            "REFRESH_TOKEN_SECRET",
            datetime.now(timezone.utc) + timedelta(hours=1),
        )
        session.commit()

    result = runner.invoke(
        app,
        [
            "shopee",
            "run-plan-all",
            "--plan",
            str(plan_path),
            "--save-root",
            str(tmp_path / "artifacts"),
            "--no-print",
        ],
    )
    assert result.exit_code == 0
    assert "shop=shop_a" in result.output
    assert "shop=shop_b" in result.output


def test_run_plan_dry_run_no_http(monkeypatch, tmp_path) -> None:
    _setup_env(tmp_path, monkeypatch)
    monkeypatch.chdir(tmp_path)

    plan_path = tmp_path / "plan.yaml"
    plan_path.write_text(
        "\n".join(
            [
                "version: 1",
                "name: ads_probe",
                "calls:",
                "  - name: shop_info",
                "    path: /api/v2/shop/get_shop_info",
                "    params:",
                "      date: \"{{date}}\"",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(cli_module, "_build_shopee_client", lambda settings: None)

    result = runner.invoke(
        app,
        [
            "shopee",
            "run-plan",
            "--shop",
            "shop_a",
            "--plan",
            str(plan_path),
            "--vars",
            "date=2026-02-01",
            "--dry-run",
        ],
    )
    assert result.exit_code == 0
    assert "dry-run shop=shop_a" in result.output
    assert "params=['date']" in result.output or "params=[date]" in result.output


def test_run_plan_all_only_shops(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "shopee.db"
    shops_path = tmp_path / "shops.yaml"
    shops_path.write_text(
        "\n".join(
            [
                "- shop_key: shop_a",
                "  label: SHOP_A",
                "  enabled: false",
                "  shopee_shop_id: 123456",
                "- shop_key: shop_b",
                "  label: SHOP_B",
                "  enabled: false",
                "  shopee_shop_id: 654321",
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
    monkeypatch.chdir(tmp_path)

    plan_path = tmp_path / "plan.yaml"
    plan_path.write_text(
        "\n".join(
            [
                "version: 1",
                "name: ads_probe",
                "calls:",
                "  - name: shop_info",
                "    path: /api/v2/shop/get_shop_info",
                "    params: {}",
            ]
        ),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "shopee",
            "run-plan-all",
            "--plan",
            str(plan_path),
            "--only-shops",
            "shop_b",
            "--dry-run",
        ],
    )
    assert result.exit_code == 0
    assert "shop=shop_b" in result.output
    assert "shop=shop_a" not in result.output
