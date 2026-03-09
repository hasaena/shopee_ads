from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

from fastapi.testclient import TestClient

from dotori_shopee_automation.config import get_settings
from dotori_shopee_automation.webapp import app


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURES_ADS = REPO_ROOT / "tests" / "fixtures" / "shopee_ads"
FIXTURES_ALERTS = REPO_ROOT / "tests" / "fixtures" / "shopee_ads_alerts" / "open"


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
    db_path = tmp_path / "task076.db"
    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env["DATABASE_URL"] = f"sqlite:///{db_path.as_posix()}"
    env["OPS_TOKEN"] = "task076-ops-token"
    env["SHOPEE_SAMORD_SHOP_ID"] = "497412318"
    env["SHOPEE_MINMIN_SHOP_ID"] = "567655304"
    env.pop("ALLOW_NETWORK", None)
    return env


def _run_cli(env: dict[str, str], args: list[str]) -> subprocess.CompletedProcess[str]:
    cmd = [sys.executable, "-m", "dotori_shopee_automation.cli", *args]
    return subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def _parse_report_paths(output: str) -> dict[str, Path]:
    rows: dict[str, Path] = {}
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line.startswith("report_path shop="):
            continue
        rest = line.replace("report_path shop=", "", 1)
        if " path=" not in rest:
            continue
        shop, report_path = rest.split(" path=", 1)
        rows[shop.strip()] = Path(report_path.strip())
    return rows


def test_token_push_then_schedule_alerts_db_mode_e2e(tmp_path: Path, monkeypatch) -> None:
    env = _base_env(tmp_path)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()

    now_ts = int(time.time())
    payload = {
        "tokens": {
            "497412318": {
                "access_token": "A" * 104,
                "refresh_token": "R" * 105,
                "expire_timestamp": now_ts + 7200,
                "refresh_token_expire_timestamp": now_ts + 2_592_000,
            },
            "567655304": {
                "access_token": "B" * 105,
                "refresh_token": "S" * 106,
                "expire_timestamp": now_ts + 7100,
                "refresh_token_expire_timestamp": now_ts + 2_591_900,
            },
            "999999999": {
                "access_token": "IGNORED_ACCESS_TOKEN",
                "refresh_token": "IGNORED_REFRESH_TOKEN",
                "expire_timestamp": now_ts + 7000,
                "refresh_token_expire_timestamp": now_ts + 2_591_800,
            },
        }
    }

    with TestClient(app) as client:
        response = client.post(
            "/ops/phase1/token/import",
            json=payload,
            headers={"Authorization": "Bearer task076-ops-token"},
        )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["ok"] is True
    assert body["imported_total"] == 2
    assert sorted(body["updated_shops"]) == ["minmin", "samord"]
    assert body["ignored_shop_ids"] == ["999999999"]

    preflight = _run_cli(
        env,
        [
            "ops",
            "phase1",
            "token",
            "preflight",
            "--shops",
            "samord,minmin",
            "--min-access-ttl-sec",
            "600",
        ],
    )
    assert preflight.returncode == 0, preflight.stdout + preflight.stderr
    assert "token_source=db" in preflight.stdout
    assert "token_len=" in preflight.stdout
    assert "token_sha8=" in preflight.stdout
    assert "preflight_ok=1" in preflight.stdout

    reports_dir = tmp_path / "reports"
    schedule = _run_cli(
        env,
        [
            "ops",
            "phase1",
            "schedule",
            "run-once",
            "--job",
            "daily-midday",
            "--date",
            "2026-02-03",
            "--shops",
            "samord,minmin",
            "--transport",
            "fixtures",
            "--fixtures-dir",
            str(FIXTURES_ADS),
            "--reports-dir",
            str(reports_dir),
            "--no-send-discord",
        ],
    )
    assert schedule.returncode == 0, schedule.stdout + schedule.stderr
    assert "token_source=db" in schedule.stdout
    assert "preflight_ok=1" in schedule.stdout
    assert "phase1_schedule_run_once_ok=1" in schedule.stdout

    report_paths = _parse_report_paths(schedule.stdout)
    assert set(report_paths.keys()) == {"samord", "minmin"}
    for report_path in report_paths.values():
        assert report_path.exists()
        assert report_path.stat().st_size > 0

    alerts = _run_cli(
        env,
        [
            "ops",
            "phase1",
            "alerts",
            "live-smoke",
            "--db",
            str(tmp_path / "task076.db"),
            "--shops",
            "samord,minmin",
            "--transport",
            "fixtures",
            "--fixtures-dir",
            str(FIXTURES_ALERTS),
            "--no-send-discord",
        ],
    )
    assert alerts.returncode == 0, alerts.stdout + alerts.stderr
    assert "token_source=db" in alerts.stdout
    assert "preflight_ok=1" in alerts.stdout
    assert "phase1_alerts_run_once_ok=1" in alerts.stdout

    get_settings.cache_clear()
