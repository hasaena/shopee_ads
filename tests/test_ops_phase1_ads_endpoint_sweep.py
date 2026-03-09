from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = REPO_ROOT / "tests" / "fixtures" / "shopee_ads"

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
    "FAKE_ACCESS_TOKEN_SHOULD_BE_REDACTED",
    "FAKE_BEARER_TOKEN_SHOULD_BE_REDACTED",
    "FAKE_PARTNER_KEY_SHOULD_BE_REDACTED",
]


def _write_shops(path: Path) -> None:
    lines = [
        "- shop_key: samord",
        "  label: SAMORD",
        "  enabled: true",
        "  shopee_shop_id: 111",
        "- shop_key: minmin",
        "  label: MINMIN",
        "  enabled: true",
        "  shopee_shop_id: 222",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def test_ops_phase1_ads_endpoint_sweep_fixtures(tmp_path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)

    candidates_path = tmp_path / "ads_candidates.yaml"
    candidates_path.write_text(
        "\n".join(
            [
                "daily:",
                "  - /api/v2/ads/get_all_cpc_ads_daily_performance",
                "snapshot:",
                "  - /api/v2/ads/get_total_balance",
            ]
        ),
        encoding="utf-8",
    )

    out_md = tmp_path / "sweep.md"

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env.pop("ALLOW_NETWORK", None)

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "ads-endpoint",
        "sweep",
        "--date",
        "2026-02-03",
        "--shops",
        "samord,minmin",
        "--transport",
        "fixtures",
        "--candidates",
        str(candidates_path),
        "--fixtures-dir",
        str(FIXTURE_ROOT),
        "--out-md",
        str(out_md),
    ]

    result = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    stdout = result.stdout
    assert "ads_endpoint_sweep_start" in stdout
    assert "recommended_ads_daily_path=" in stdout
    assert "recommended_ads_snapshot_path=" in stdout
    assert "reachable=1" in stdout
    assert "query_keys=access_token,end_date,partner_id,shop_id,sign,start_date,timestamp" in stdout
    assert "query_keys=access_token,partner_id,shop_id,sign,timestamp" in stdout
    assert out_md.exists()
    for marker in SECRET_MARKERS:
        assert marker not in stdout


def test_ops_phase1_ads_endpoint_sweep_reachability(tmp_path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)

    candidates_path = tmp_path / "ads_candidates.yaml"
    candidates_path.write_text(
        "\n".join(
            [
                "daily:",
                "  - path: /api/v2/ads/daily_404",
                "    status: 404",
                "  - path: /api/v2/ads/daily_401",
                "    status: 401",
                "  - path: /api/v2/ads/daily_denied",
                "    fixture: ads_denied.json",
                "snapshot:",
                "  - path: /api/v2/ads/snapshot_403",
                "    status: 403",
            ]
        ),
        encoding="utf-8",
    )

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env.pop("ALLOW_NETWORK", None)

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "ads-endpoint",
        "sweep",
        "--date",
        "2026-02-03",
        "--shops",
        "samord,minmin",
        "--transport",
        "fixtures",
        "--candidates",
        str(candidates_path),
        "--fixtures-dir",
        str(FIXTURE_ROOT),
    ]

    result = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    stdout = result.stdout
    assert "reachable=0" in stdout
    assert "reason=path_not_found" in stdout
    assert "reason=auth_failed" in stdout
    assert "reason=shopee_error_1" in stdout
    for marker in SECRET_MARKERS:
        assert marker not in stdout
