from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.webapp import app


def _write_phase1_shops(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "- shop_key: samord",
                "  label: SAMORD",
                "  enabled: true",
                "  shopee_shop_id: 497412318",
                "- shop_key: minmin",
                "  label: MINMIN",
                "  enabled: true",
                "  shopee_shop_id: 567655304",
            ]
        ),
        encoding="utf-8",
    )


def _configure_test_env(monkeypatch, tmp_path: Path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_phase1_shops(shops_path)
    db_path = tmp_path / "token_ping.db"
    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path.as_posix()}")
    monkeypatch.setenv("OPS_TOKEN", "ops-secret")
    monkeypatch.setenv("SHOPEE_SAMORD_SHOP_ID", "497412318")
    monkeypatch.setenv("SHOPEE_MINMIN_SHOP_ID", "567655304")
    get_settings.cache_clear()


def test_token_ping_requires_auth(tmp_path: Path, monkeypatch) -> None:
    _configure_test_env(monkeypatch, tmp_path)
    with TestClient(app) as client:
        response = client.get("/ops/phase1/token/ping")
    assert response.status_code in {401, 403}
    get_settings.cache_clear()


def test_token_ping_authorized_safe_response(tmp_path: Path, monkeypatch) -> None:
    _configure_test_env(monkeypatch, tmp_path)
    with TestClient(app) as client:
        response = client.get(
            "/ops/phase1/token/ping",
            headers={"Authorization": "Bearer ops-secret"},
        )
    assert response.status_code == 200, response.text
    data = response.json()
    assert data.get("ok") is True
    assert data.get("phase") == "phase1"
    assert sorted(data.get("shops") or []) == ["minmin", "samord"]
    assert data.get("auth") == "ok"
    body = response.text
    assert "access_token" not in body
    assert "refresh_token" not in body
    get_settings.cache_clear()
