from __future__ import annotations

import hashlib
import json
from pathlib import Path

from fastapi.testclient import TestClient

from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.webapp import app


TOKEN_FILE = (
    Path(__file__).resolve().parents[1]
    / "tests"
    / "fixtures"
    / "appsscript_tokens"
    / "shopee_tokens_export_example.json"
)


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
    db_path = tmp_path / "token_status_after_import.db"
    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path.as_posix()}")
    monkeypatch.setenv("OPS_TOKEN", "ops-secret")
    monkeypatch.setenv("SHOPEE_SAMORD_SHOP_ID", "497412318")
    monkeypatch.setenv("SHOPEE_MINMIN_SHOP_ID", "567655304")
    get_settings.cache_clear()


def _sha8(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:8]


def test_token_status_after_import_has_fingerprint_only(tmp_path: Path, monkeypatch) -> None:
    _configure_test_env(monkeypatch, tmp_path)
    tokens = json.loads(TOKEN_FILE.read_text(encoding="utf-8"))
    payload = {"tokens": tokens}

    with TestClient(app) as client:
        imported = client.post(
            "/ops/phase1/token/import",
            json=payload,
            headers={"Authorization": "Bearer ops-secret"},
        )
        status = client.get(
            "/ops/phase1/token/status",
            headers={"Authorization": "Bearer ops-secret"},
        )

    assert imported.status_code == 200, imported.text
    body = imported.json()
    assert body.get("ok") is True
    assert int(body.get("imported_total", 0)) == 2
    assert sorted(body.get("updated_shops", [])) == ["minmin", "samord"]

    assert status.status_code == 200, status.text
    data = status.json()
    shops = data.get("shops") or {}
    assert sorted(shops.keys()) == ["minmin", "samord"]
    assert data.get("token_mode") == "legacy"

    expected_access = tokens["SHOPEE_TOKEN_DATA_497412318"]["access_token"]
    expected_sha = _sha8(expected_access)
    for shop_key in ("samord", "minmin"):
        row = shops[shop_key]
        assert row["token_len"] == len(expected_access)
        assert row["token_sha8"] == expected_sha
        assert row["updated_at"] is not None
        assert row["has_refresh_token"] == 0
        assert row["token_mode"] in {"legacy", "access_only"}

    text = status.text
    assert expected_access not in text
    assert tokens["SHOPEE_TOKEN_DATA_497412318"]["refresh_token"] not in text
    assert "access_token" not in text
    assert '"refresh_token":' not in text
    get_settings.cache_clear()
