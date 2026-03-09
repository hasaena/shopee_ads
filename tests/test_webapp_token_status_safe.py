from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

from fastapi.testclient import TestClient

from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.db import EventLog, SessionLocal, init_db
from dotori_shopee_automation.shopee.token_store import upsert_token
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
                "- shop_key: extra",
                "  label: EXTRA",
                "  enabled: true",
                "  shopee_shop_id: 999999999",
            ]
        ),
        encoding="utf-8",
    )


def _configure_test_env(monkeypatch, tmp_path: Path) -> Path:
    shops_path = tmp_path / "shops.yaml"
    _write_phase1_shops(shops_path)
    db_path = tmp_path / "token_status_safe.db"
    monkeypatch.setenv("SHOPS_CONFIG_PATH", str(shops_path))
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path.as_posix()}")
    monkeypatch.setenv("OPS_TOKEN", "ops-secret")
    monkeypatch.setenv("SHOPEE_SAMORD_SHOP_ID", "497412318")
    monkeypatch.setenv("SHOPEE_MINMIN_SHOP_ID", "567655304")
    get_settings.cache_clear()
    return db_path


def _seed_tokens_and_gate_status() -> None:
    now = datetime.now(timezone.utc)
    init_db()
    session = SessionLocal()
    try:
        upsert_token(
            session,
            "samord",
            497412318,
            "SAMORD_ACCESS_SAFE",
            "SAMORD_REFRESH_SAFE",
            now + timedelta(minutes=20),
            now + timedelta(days=30),
        )
        upsert_token(
            session,
            "minmin",
            567655304,
            "MINMIN_ACCESS_SAFE",
            "MINMIN_REFRESH_SAFE",
            now + timedelta(minutes=25),
            now + timedelta(days=30),
        )
        upsert_token(
            session,
            "extra",
            999999999,
            "EXTRA_ACCESS_SHOULD_NOT_APPEAR",
            "EXTRA_REFRESH_SHOULD_NOT_APPEAR",
            now + timedelta(minutes=30),
            now + timedelta(days=30),
        )
        session.add(
            EventLog(
                level="WARN",
                message="token_preflight_gate_status",
                meta_json=json.dumps(
                    {
                        "shop_key": "samord",
                        "shop_label": "SAMORD",
                        "shop_id": 497412318,
                        "last_verdict": "expired",
                        "last_alert_at": int(now.timestamp()) - 30,
                        "last_resolved_at": -1,
                        "cooldown_until": int(now.timestamp()) + 1200,
                        "resolved_cooldown_until": -1,
                        "min_required_ttl_sec": 1200,
                        "last_access_ttl_sec": -30,
                        "gate_state": "blocked",
                        "updated_at": int(now.timestamp()),
                    },
                    ensure_ascii=True,
                ),
            )
        )
        session.add(
            EventLog(
                level="INFO",
                message="token_preflight_gate_status",
                meta_json=json.dumps(
                    {
                        "shop_key": "minmin",
                        "shop_label": "MINMIN",
                        "shop_id": 567655304,
                        "last_verdict": "ok",
                        "last_alert_at": int(now.timestamp()) - 90,
                        "last_resolved_at": int(now.timestamp()) - 60,
                        "cooldown_until": int(now.timestamp()) + 600,
                        "resolved_cooldown_until": int(now.timestamp()) + 1800,
                        "min_required_ttl_sec": 1200,
                        "last_access_ttl_sec": 3600,
                        "gate_state": "ok",
                        "updated_at": int(now.timestamp()),
                    },
                    ensure_ascii=True,
                ),
            )
        )
        session.commit()
    finally:
        session.close()


def test_token_status_returns_phase1_safe_fields_only(tmp_path: Path, monkeypatch) -> None:
    _configure_test_env(monkeypatch, tmp_path)
    _seed_tokens_and_gate_status()

    with TestClient(app) as client:
        response = client.get(
            "/ops/phase1/token/status",
            headers={"Authorization": "Bearer ops-secret"},
        )
    assert response.status_code == 200, response.text
    data = response.json()
    assert data.get("ok") is True
    assert data.get("phase") == "phase1"

    shops = data.get("shops") or {}
    assert sorted(shops.keys()) == ["minmin", "samord"]

    samord = shops["samord"]
    minmin = shops["minmin"]
    assert samord["shop_id"] == 497412318
    assert minmin["shop_id"] == 567655304
    assert int(samord["token_len"]) > 0
    assert int(minmin["token_len"]) > 0
    assert isinstance(samord["token_sha8"], str) and len(samord["token_sha8"]) == 8
    assert isinstance(minmin["token_sha8"], str) and len(minmin["token_sha8"]) == 8
    assert isinstance(samord["access_expires_in_sec"], int)
    assert isinstance(minmin["access_expires_in_sec"], int)
    assert samord["updated_at"] is not None
    assert minmin["updated_at"] is not None
    assert samord["gate_state"] == "blocked"
    assert minmin["gate_state"] == "ok"
    assert samord["token_mode"] == "legacy"
    assert minmin["token_mode"] == "legacy"
    assert samord["has_refresh_token"] == 1
    assert minmin["has_refresh_token"] == 1
    assert int(samord["refresh_expires_in_sec"]) > 0
    assert int(minmin["refresh_expires_in_sec"]) > 0
    assert samord["cooldown_until"] is not None
    assert minmin["resolved_cooldown_until"] is not None

    body = response.text
    assert "access_token" not in body
    assert '"refresh_token":' not in body
    assert "SAMORD_ACCESS_SAFE" not in body
    assert "MINMIN_ACCESS_SAFE" not in body
    assert "EXTRA_ACCESS_SHOULD_NOT_APPEAR" not in body
    get_settings.cache_clear()
