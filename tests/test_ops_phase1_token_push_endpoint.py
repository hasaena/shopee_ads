from __future__ import annotations

import json
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
    db_path = tmp_path / "token_push_endpoint.db"
    _write_phase1_shops(shops_path)
    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path.as_posix()}")
    monkeypatch.setenv("OPS_TOKEN", "ops-secret")
    monkeypatch.setenv("SHOPEE_SAMORD_SHOP_ID", "497412318")
    monkeypatch.setenv("SHOPEE_MINMIN_SHOP_ID", "567655304")
    get_settings.cache_clear()


def test_token_push_endpoint_accepts_appsscript_push_shape(tmp_path: Path, monkeypatch) -> None:
    _configure_test_env(monkeypatch, tmp_path)
    push_payload = {
        "source": "appsscript_push",
        "pushed_at": 1_700_000_000,
        "tokens": {
            "SHOPEE_TOKEN_DATA_497412318": json.dumps(
                {
                    "access_token": "A" * 105,
                    "refresh_token": "R" * 106,
                    "expire_timestamp": 2_000_000_000,
                    "refresh_token_expire_timestamp": 2_002_592_000,
                }
            ),
            "SHOPEE_TOKEN_DATA_567655304": {
                "access_token": "B" * 105,
                "refresh_token": "S" * 106,
                "expire_timestamp": 2_000_000_100,
                "refresh_token_expire_timestamp": 2_002_592_100,
            },
            "SHOPEE_TOKEN_DATA_999999999": {
                "access_token": "IGNORED_ACCESS",
                "refresh_token": "IGNORED_REFRESH",
                "expire_timestamp": 2_000_000_200,
                "refresh_token_expire_timestamp": 2_002_592_200,
            },
        },
    }

    with TestClient(app) as client:
        response = client.post(
            "/ops/phase1/token/import",
            json=push_payload,
            headers={"Authorization": "Bearer ops-secret"},
        )

    assert response.status_code == 200, response.text
    data = response.json()
    assert data["ok"] is True
    assert data["source"] == "appsscript_push"
    assert data["pushed_at"] == 1_700_000_000
    assert data["imported"] == 2
    assert data["noop"] == 0
    assert sorted(data["shops"]) == ["minmin", "samord"]
    assert data["ignored_shop_ids"] == ["999999999"]
    assert len(data["token_sha8"]["samord"]) == 8
    assert len(data["token_sha8"]["minmin"]) == 8
    assert data["token_fingerprints"]["samord"]["token_len"] == 105
    assert data["imported_total"] == 2
    assert data["noop_total"] == 0
    assert "A" * 20 not in response.text
    assert "B" * 20 not in response.text
    assert "IGNORED_ACCESS" not in response.text
    get_settings.cache_clear()


def test_token_push_endpoint_requires_ops_auth(tmp_path: Path, monkeypatch) -> None:
    _configure_test_env(monkeypatch, tmp_path)
    payload = {
        "source": "appsscript_push",
        "tokens": {
            "497412318": {
                "access_token": "A" * 105,
                "refresh_token": "R" * 106,
                "expire_timestamp": 2_000_000_000,
                "refresh_token_expire_timestamp": 2_002_592_000,
            }
        },
    }

    with TestClient(app) as client:
        response = client.post("/ops/phase1/token/import", json=payload)

    assert response.status_code in {401, 403}
    get_settings.cache_clear()
