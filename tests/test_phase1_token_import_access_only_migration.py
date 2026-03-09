from __future__ import annotations

import sqlite3
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


def _configure_test_env(monkeypatch, tmp_path: Path) -> Path:
    shops_path = tmp_path / "shops.yaml"
    _write_phase1_shops(shops_path)
    db_path = tmp_path / "access_only_migration.db"

    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path.as_posix()}")
    monkeypatch.setenv("OPS_TOKEN", "ops-secret")
    monkeypatch.setenv("SHOPEE_SAMORD_SHOP_ID", "497412318")
    monkeypatch.setenv("SHOPEE_MINMIN_SHOP_ID", "567655304")
    get_settings.cache_clear()
    return db_path


def _seed_legacy_refresh_rows(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS shopee_tokens (
                shop_key TEXT PRIMARY KEY,
                shop_id INTEGER NOT NULL,
                access_token TEXT NOT NULL,
                refresh_token TEXT NOT NULL,
                access_token_expires_at DATETIME,
                refresh_token_expires_at DATETIME,
                updated_at DATETIME
            )
            """
        )
        cur.execute(
            """
            INSERT OR REPLACE INTO shopee_tokens
            (shop_key, shop_id, access_token, refresh_token, access_token_expires_at, refresh_token_expires_at, updated_at)
            VALUES (?, ?, ?, ?, datetime('now', '+2 hours'), datetime('now', '+30 days'), datetime('now'))
            """,
            ("samord", 497412318, "OLD_ACCESS_SAMORD", "OLD_REFRESH_SAMORD"),
        )
        cur.execute(
            """
            INSERT OR REPLACE INTO shopee_tokens
            (shop_key, shop_id, access_token, refresh_token, access_token_expires_at, refresh_token_expires_at, updated_at)
            VALUES (?, ?, ?, ?, datetime('now', '+2 hours'), datetime('now', '+30 days'), datetime('now'))
            """,
            ("minmin", 567655304, "OLD_ACCESS_MINMIN", "OLD_REFRESH_MINMIN"),
        )
        conn.commit()
    finally:
        conn.close()


def _load_rows(db_path: Path) -> dict[str, tuple[str, str | None, str | None]]:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT shop_key, access_token, refresh_token, refresh_token_expires_at "
            "FROM shopee_tokens ORDER BY shop_key ASC"
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    return {
        str(row[0]): (
            str(row[1]),
            str(row[2]) if row[2] is not None else None,
            str(row[3]) if row[3] is not None else None,
        )
        for row in rows
    }


def test_access_only_import_clears_preexisting_refresh_tokens(tmp_path: Path, monkeypatch) -> None:
    db_path = _configure_test_env(monkeypatch, tmp_path)
    _seed_legacy_refresh_rows(db_path)

    payload = {
        "source": "appsscript_push_access_only",
        "token_mode": "access_only",
        "shops": {
            "samord": {
                "shop_id": 497412318,
                "access_token": "ACCESS_SAM_NEW_1234567890",
                "expire_timestamp": 2_000_000_000,
            },
            "minmin": {
                "shop_id": 567655304,
                "access_token": "ACCESS_MIN_NEW_1234567890",
                "expire_timestamp": 2_000_000_000,
            },
        },
    }

    with TestClient(app) as client:
        imported = client.post(
            "/ops/phase1/token/import",
            json=payload,
            headers={"Authorization": "Bearer ops-secret"},
        )
        token_status = client.get(
            "/ops/phase1/token/status",
            headers={"Authorization": "Bearer ops-secret"},
        )

    assert imported.status_code == 200, imported.text
    import_data = imported.json()
    assert import_data.get("ok") is True
    assert import_data.get("token_mode") == "access_only"
    assert int(import_data.get("imported_total") or 0) == 2
    assert int(import_data.get("discarded_refresh_tokens") or 0) == 0
    assert bool((import_data.get("auto_resume") or {}).get("checked")) is True

    db_rows = _load_rows(db_path)
    assert db_rows["samord"][0] == "ACCESS_SAM_NEW_1234567890"
    assert db_rows["minmin"][0] == "ACCESS_MIN_NEW_1234567890"
    assert db_rows["samord"][1] == ""
    assert db_rows["minmin"][1] == ""
    assert db_rows["samord"][2] is None
    assert db_rows["minmin"][2] is None

    assert token_status.status_code == 200, token_status.text
    status_data = token_status.json()
    shops = status_data.get("shops") or {}
    assert shops["samord"]["has_refresh_token"] == 0
    assert shops["minmin"]["has_refresh_token"] == 0
    assert shops["samord"]["token_mode"] == "access_only"
    assert shops["minmin"]["token_mode"] == "access_only"
    assert int(shops["samord"]["access_expires_in_sec"]) > 0
    assert int(shops["minmin"]["access_expires_in_sec"]) > 0

    get_settings.cache_clear()
