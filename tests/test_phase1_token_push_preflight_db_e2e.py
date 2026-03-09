from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from fastapi.testclient import TestClient

from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.webapp import app


REPO_ROOT = Path(__file__).resolve().parents[1]


def _write_shops(path: Path) -> None:
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


def _base_env(tmp_path: Path) -> dict[str, str]:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)
    db_path = tmp_path / "task075.db"
    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env["DATABASE_URL"] = f"sqlite:///{db_path.as_posix()}"
    env["OPS_TOKEN"] = "test-ops-token"
    env["SHOPEE_SAMORD_SHOP_ID"] = "497412318"
    env["SHOPEE_MINMIN_SHOP_ID"] = "567655304"
    return env


def _run_db_preflight(env: dict[str, str], min_ttl: int = 600) -> subprocess.CompletedProcess:
    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "token",
        "preflight",
        "--shops",
        "samord,minmin",
        "--min-access-ttl-sec",
        str(min_ttl),
    ]
    return subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_token_push_to_db_then_db_preflight_ok(tmp_path: Path, monkeypatch) -> None:
    env = _base_env(tmp_path)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()

    payload = {
        "tokens": {
            "497412318": {
                "access_token": "A" * 104,
                "refresh_token": "R" * 105,
                "expire_timestamp": 2_000_000_000,
                "refresh_token_expire_timestamp": 2_002_592_000,
            },
            "567655304": {
                "access_token": "B" * 105,
                "refresh_token": "S" * 106,
                "expire_timestamp": 2_000_000_100,
                "refresh_token_expire_timestamp": 2_002_592_100,
            },
            "999999999": {
                "access_token": "IGNORED_ACCESS",
                "refresh_token": "IGNORED_REFRESH",
                "expire_timestamp": 2_000_000_200,
                "refresh_token_expire_timestamp": 2_002_592_200,
            },
        }
    }
    with TestClient(app) as client:
        response = client.post(
            "/ops/phase1/token/import",
            json=payload,
            headers={"Authorization": "Bearer test-ops-token"},
        )
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["ok"] is True
    assert data["imported_total"] == 2
    assert sorted(data["updated_shops"]) == ["minmin", "samord"]
    assert data["ignored_shop_ids"] == ["999999999"]

    result = _run_db_preflight(env=env, min_ttl=600)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "token_source=db" in result.stdout
    assert "preflight_ok=1" in result.stdout
    assert "A" * 20 not in result.stdout
    assert "B" * 20 not in result.stdout
    get_settings.cache_clear()


def test_db_preflight_fails_for_expired_tokens(tmp_path: Path, monkeypatch) -> None:
    env = _base_env(tmp_path)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()

    payload = {
        "497412318": {
            "access_token": "Z" * 104,
            "refresh_token": "Y" * 105,
            "expire_timestamp": 1,
            "refresh_token_expire_timestamp": 2_002_592_000,
        },
        "567655304": {
            "access_token": "W" * 105,
            "refresh_token": "V" * 106,
            "expire_timestamp": 1,
            "refresh_token_expire_timestamp": 2_002_592_100,
        },
    }
    with TestClient(app) as client:
        response = client.post(
            "/ops/phase1/token/import",
            json=payload,
            headers={"X-OPS-TOKEN": "test-ops-token"},
        )
    assert response.status_code == 200, response.text

    result = _run_db_preflight(env=env, min_ttl=600)
    assert result.returncode == 2, result.stdout + result.stderr
    assert "token_source=db" in result.stdout
    assert "token_verdict=expired" in result.stdout
    assert "preflight_ok=0" in result.stdout
    assert "Z" * 20 not in result.stdout
    assert "W" * 20 not in result.stdout
    get_settings.cache_clear()
