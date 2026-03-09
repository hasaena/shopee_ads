from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.db import EventLog, SessionLocal, init_db
from dotori_shopee_automation.shopee.token_store import upsert_token
from dotori_shopee_automation.webapp import app
from dotori_shopee_automation import webapp as webapp_module


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
                "- shop_key: haena",
                "  label: HAENA",
                "  enabled: true",
                "  shopee_shop_id: 820977786",
            ]
        ),
        encoding="utf-8",
    )


def _configure_test_env(monkeypatch, tmp_path: Path) -> Path:
    shops_path = tmp_path / "shops.yaml"
    _write_phase1_shops(shops_path)
    db_path = tmp_path / "token_import.db"

    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path.as_posix()}")
    monkeypatch.setenv("OPS_TOKEN", "ops-secret")
    monkeypatch.setenv("SHOPEE_SAMORD_SHOP_ID", "497412318")
    monkeypatch.setenv("SHOPEE_MINMIN_SHOP_ID", "567655304")
    get_settings.cache_clear()
    return db_path


def _count_token_rows(db_path: Path) -> int:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM shopee_tokens")
        return int(cur.fetchone()[0])
    finally:
        conn.close()


def _load_refresh_columns(db_path: Path) -> dict[str, tuple[str | None, str | None]]:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT shop_key, refresh_token, refresh_token_expires_at "
            "FROM shopee_tokens ORDER BY shop_key ASC"
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    return {
        str(row[0]): (
            str(row[1]) if row[1] is not None else None,
            str(row[2]) if row[2] is not None else None,
        )
        for row in rows
    }


def _seed_blocked_gate_state(now_utc: datetime) -> None:
    init_db()
    session = SessionLocal()
    try:
        for shop_key, shop_id in (("samord", 497412318), ("minmin", 567655304)):
            upsert_token(
                session,
                shop_key,
                shop_id,
                f"EXPIRED_{shop_key}",
                f"REFRESH_{shop_key}",
                now_utc - timedelta(minutes=5),
                now_utc + timedelta(days=7),
            )
            session.add(
                EventLog(
                    level="WARN",
                    message="token_preflight_gate_status",
                    meta_json=json.dumps(
                        {
                            "shop_key": shop_key,
                            "shop_label": shop_key.upper(),
                            "shop_id": shop_id,
                            "last_verdict": "expired",
                            "last_alert_at": int(now_utc.timestamp()) - 120,
                            "last_resolved_at": -1,
                            "cooldown_until": int(now_utc.timestamp()) + 1200,
                            "resolved_cooldown_until": -1,
                            "min_required_ttl_sec": 900,
                            "last_access_ttl_sec": -300,
                            "gate_state": "blocked",
                            "updated_at": int(now_utc.timestamp()),
                        },
                        ensure_ascii=True,
                    ),
                )
            )
        session.commit()
    finally:
        session.close()


def test_token_import_ok_filters_phase1_shops(tmp_path: Path, monkeypatch) -> None:
    db_path = _configure_test_env(monkeypatch, tmp_path)
    payload = {
        "497412318": {
            "access_token": "a" * 104,
            "refresh_token": "r" * 105,
            "expire_timestamp": 2_000_000_000,
            "refresh_token_expire_timestamp": 2_002_592_000,
        },
        "567655304": {
            "access_token": "b" * 105,
            "refresh_token": "s" * 106,
            "expire_timestamp": 2_000_000_001,
            "refresh_token_expire_timestamp": 2_002_592_001,
        },
        "820977786": {
            "access_token": "ignored-access",
            "refresh_token": "ignored-refresh",
            "expire_timestamp": 2_000_000_002,
            "refresh_token_expire_timestamp": 2_002_592_002,
        },
    }

    with TestClient(app) as client:
        response = client.post(
            "/ops/phase1/token/import",
            json=payload,
            headers={"Authorization": "Bearer ops-secret"},
        )

    assert response.status_code == 200, response.text
    data = response.json()
    assert data["ok"] is True
    assert data["imported_total"] == 2
    assert sorted(data["updated_shops"]) == ["minmin", "samord"]
    assert data["ignored_shop_ids"] == ["820977786"]
    assert data["token_fingerprints"]["samord"]["token_len"] == 104
    assert len(data["token_fingerprints"]["samord"]["token_sha8"]) == 8
    assert data["discarded_refresh_tokens"] == 2
    assert "a" * 32 not in response.text
    assert "b" * 32 not in response.text
    assert _count_token_rows(db_path) == 2
    get_settings.cache_clear()


def test_token_import_rejects_missing_auth(tmp_path: Path, monkeypatch) -> None:
    _configure_test_env(monkeypatch, tmp_path)
    payload = {
        "497412318": {
            "access_token": "a" * 104,
            "refresh_token": "r" * 105,
            "expire_timestamp": 2_000_000_000,
            "refresh_token_expire_timestamp": 2_002_592_000,
        }
    }

    with TestClient(app) as client:
        response = client.post("/ops/phase1/token/import", json=payload)

    assert response.status_code in {401, 403}
    get_settings.cache_clear()


def test_token_import_rejects_malformed_payload(tmp_path: Path, monkeypatch) -> None:
    _configure_test_env(monkeypatch, tmp_path)
    payload = {
        "tokens": {
            "497412318": {
                "access_token": "",
                "refresh_token": "r" * 105,
                "expire_timestamp": 2_000_000_000,
            }
        }
    }

    with TestClient(app) as client:
        response = client.post(
            "/ops/phase1/token/import",
            json=payload,
            headers={"X-OPS-TOKEN": "ops-secret"},
        )

    assert response.status_code == 400
    assert "invalid payload" in response.text
    get_settings.cache_clear()


def test_token_import_access_only_auto_resumes_gate(tmp_path: Path, monkeypatch) -> None:
    db_path = _configure_test_env(monkeypatch, tmp_path)
    seeded_now = datetime.now(timezone.utc)
    _seed_blocked_gate_state(seeded_now)
    fresh_expire_ts = int((seeded_now + timedelta(hours=4)).timestamp())
    payload = {
        "version": 1,
        "token_mode": "access_only",
        "source": "appsscript_push_access_only",
        "shops": {
            "497412318": {
                "access_token": "A" * 104,
                "expire_timestamp": fresh_expire_ts,
            },
            "567655304": {
                "access_token": "B" * 105,
                "expire_timestamp": fresh_expire_ts,
            },
        },
    }

    with TestClient(app) as client:
        imported = client.post(
            "/ops/phase1/token/import",
            json=payload,
            headers={"Authorization": "Bearer ops-secret"},
        )
        status = client.get(
            "/ops/phase1/status",
            headers={"Authorization": "Bearer ops-secret"},
        )

    assert imported.status_code == 200, imported.text
    import_body = imported.json()
    assert import_body["ok"] is True
    assert import_body["token_mode"] == "access_only"
    assert import_body["discarded_refresh_tokens"] == 0
    assert import_body["auto_resume"]["checked"] is True
    assert import_body["auto_resume"]["auto_resumed"] is True
    assert import_body["auto_resume"]["blocked_after"] is False

    assert status.status_code == 200, status.text
    status_body = status.json()
    assert status_body["paused"] is False
    assert status_body["paused_reason"] is None
    assert status_body["token_import_last_at"] is not None
    assert status_body["token_import_last_request_id"] is not None
    assert status_body["next_action"] is None
    issue_codes = {str(row.get("code")) for row in status_body.get("issues", [])}
    assert "TOKEN_GATE_BLOCKED" not in issue_codes
    for shop_key in ("samord", "minmin"):
        row = (status_body.get("token") or {}).get(shop_key) or {}
        assert row.get("gate_state") == "ok"
        assert int(row.get("access_expires_in_sec") or -1) > 0
        assert row.get("token_source") == "appsscript_push_access_only"
        assert row.get("token_import_last_at") is not None
        assert row.get("next_action") is None

    refresh_rows = _load_refresh_columns(db_path)
    assert refresh_rows["samord"][0] == ""
    assert refresh_rows["samord"][1] is None
    assert refresh_rows["minmin"][0] == ""
    assert refresh_rows["minmin"][1] is None
    get_settings.cache_clear()


def test_token_import_legacy_payload_discards_refresh_token(tmp_path: Path, monkeypatch) -> None:
    db_path = _configure_test_env(monkeypatch, tmp_path)
    payload = {
        "tokens": {
            "497412318": {
                "access_token": "X" * 90,
                "refresh_token": "RX" * 50,
                "expire_timestamp": 2_000_000_000,
                "refresh_token_expire_timestamp": 2_002_592_000,
            },
            "567655304": {
                "access_token": "Y" * 91,
                "refresh_token": "RY" * 50,
                "expire_timestamp": 2_000_000_100,
                "refresh_token_expire_timestamp": 2_002_592_100,
            },
        }
    }

    with TestClient(app) as client:
        response = client.post(
            "/ops/phase1/token/import",
            json=payload,
            headers={"X-OPS-TOKEN": "ops-secret"},
        )

    assert response.status_code == 200, response.text
    data = response.json()
    assert data["ok"] is True
    assert data["token_mode"] == "legacy"
    assert data["discarded_refresh_tokens"] == 2
    refresh_rows = _load_refresh_columns(db_path)
    assert refresh_rows["samord"][0] == ""
    assert refresh_rows["samord"][1] is None
    assert refresh_rows["minmin"][0] == ""
    assert refresh_rows["minmin"][1] is None
    get_settings.cache_clear()


def test_token_import_mapping_empty_emits_failure_alert(tmp_path: Path, monkeypatch) -> None:
    _configure_test_env(monkeypatch, tmp_path)
    payload = {
        "497412318": {
            "access_token": "a" * 104,
            "expire_timestamp": 2_000_000_000,
        }
    }
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(webapp_module, "_resolve_phase1_shop_id_map", lambda: {})
    monkeypatch.setattr(
        webapp_module,
        "_emit_token_import_failure_alert",
        lambda **kwargs: calls.append(kwargs),
    )

    with TestClient(app) as client:
        response = client.post(
            "/ops/phase1/token/import",
            json=payload,
            headers={"Authorization": "Bearer ops-secret"},
        )

    assert response.status_code == 500
    assert "phase1 shop mapping is empty" in response.text
    assert len(calls) == 1
    assert "phase1 shop mapping is empty" in str(calls[0].get("reason"))
    get_settings.cache_clear()
