from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]

SECRET_MARKERS = [
    "PARTNER_KEY_SHOULD_NOT_PRINT",
    "ACCESS_TOKEN_SHOULD_NOT_PRINT",
    "REFRESH_TOKEN_SHOULD_NOT_PRINT",
    "AUTHORIZATION_SHOULD_NOT_PRINT",
    "AUTH_SHOULD_NOT_PRINT",
    "COOKIE_SHOULD_NOT_PRINT",
    "CLIENT_SECRET_SHOULD_NOT_PRINT",
    "SIGN_SHOULD_NOT_PRINT",
    "TOKEN_SHOULD_NOT_PRINT",
]


def _write_artifact(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")


def test_ops_phase1_artifacts_summarize_failures(tmp_path: Path) -> None:
    artifacts_root = tmp_path / "artifacts"
    date_value = "2026-02-03"

    baseline_payload = {
        "__meta": {"shop_key": "samord", "call_name": "baseline_shop_info"},
        "request_meta": {
            "path": "/api/v2/shop/get_shop_info",
            "query_keys": ["partner_id", "timestamp"],
        },
        "response_meta": {"http_status": 200},
        "parsed_error": {
            "api_error": 0,
            "api_message": "success",
            "request_id": "req-base",
        },
        "response": {
            "access_token": "ACCESS_TOKEN_SHOULD_NOT_PRINT",
        },
    }
    ads_payload = {
        "__meta": {"shop_key": "samord", "call_name": "ads_daily"},
        "request_meta": {
            "path": "/api/v2/ads/get_all_cpc_ads_daily_performance",
            "query_keys": ["partner_id", "timestamp"],
        },
        "response_meta": {"http_status": 403},
        "parsed_error": {
            "api_error": "auth_failed",
            "api_message": "denied",
            "request_id": "req-ads",
        },
        "response": {
            "partner_key": "PARTNER_KEY_SHOULD_NOT_PRINT",
        },
    }

    _write_artifact(
        artifacts_root / "samord" / date_value / "baseline.json",
        baseline_payload,
    )
    _write_artifact(
        artifacts_root / "samord" / date_value / "ads.json",
        ads_payload,
    )

    out_path = tmp_path / "summary.md"
    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "artifacts",
        "summarize-failures",
        "--date",
        date_value,
        "--shops",
        "samord,minmin",
        "--artifacts-root",
        str(artifacts_root),
        "--out",
        str(out_path),
    ]
    result = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert out_path.exists()
    content = out_path.read_text(encoding="utf-8")
    assert "| samord |" in content
    assert "base_auth_ok" in content
    assert "ads_permission_or_scope" in content
    for marker in SECRET_MARKERS:
        assert marker not in content
