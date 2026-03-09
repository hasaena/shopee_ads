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


def _copy_fixture(name: str, dest: Path) -> None:
    dest.write_text(
        (FIXTURE_ROOT / name).read_text(encoding="utf-8"),
        encoding="utf-8",
    )


def test_ops_phase1_ads_endpoint_sweep_error_body(tmp_path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)

    fixtures_dir = tmp_path / "fixtures"
    fixtures_dir.mkdir()
    _copy_fixture("shop_info_ok_with_fake_secrets.json", fixtures_dir / "shop_info.json")
    _copy_fixture(
        "ads_snapshot_ok_with_fake_secrets.json",
        fixtures_dir / "ads_snapshot_ok_with_fake_secrets.json",
    )
    auth_failed_path = fixtures_dir / "ads_auth_failed.json"
    auth_failed_path.write_text(
        "\n".join(
            [
                "{",
                '  "error": "auth_failed",',
                '  "message": "denied",',
                '  "request_id": "req-123"',
                "}",
            ]
        ),
        encoding="utf-8",
    )

    candidates_path = tmp_path / "ads_candidates.yaml"
    candidates_path.write_text(
        "\n".join(
            [
                "daily:",
                "  - path: /api/v2/ads/get_all_cpc_ads_daily_performance",
                "    status: 403",
                "    fixture: ads_auth_failed.json",
                "snapshot:",
                "  - /api/v2/ads/get_total_balance",
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
        "--fixtures-dir",
        str(fixtures_dir),
        "--candidates",
        str(candidates_path),
        "--baseline-shop-info",
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
    assert "baseline_shop_info shop=samord" in stdout
    assert "api_error=auth_failed" in stdout
    assert "api_message=denied" in stdout
    assert "request_id=req-123" in stdout
    for marker in SECRET_MARKERS:
        assert marker not in stdout


def test_failure_artifacts_redacted(tmp_path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)

    fixtures_dir = tmp_path / "fixtures"
    fixtures_dir.mkdir()
    _copy_fixture("shop_info_ok_with_fake_secrets.json", fixtures_dir / "shop_info.json")
    failure_payload = "\n".join(
        [
            "{",
            '  "error": "auth_failed",',
            '  "message": "denied",',
            '  "request_id": "req-456",',
            '  "access_token": "ACCESS_TOKEN_SHOULD_NOT_PRINT",',
            '  "refresh_token": "REFRESH_TOKEN_SHOULD_NOT_PRINT",',
            '  "partner_key": "PARTNER_KEY_SHOULD_NOT_PRINT",',
            '  "sign": "SIGN_SHOULD_NOT_PRINT"',
            "}",
        ]
    )
    (fixtures_dir / "ads_failure.json").write_text(failure_payload, encoding="utf-8")

    candidates_path = tmp_path / "ads_candidates.yaml"
    candidates_path.write_text(
        "\n".join(
            [
                "daily:",
                "  - path: /api/v2/ads/get_all_cpc_ads_daily_performance",
                "    status: 403",
                "    fixture: ads_failure.json",
                "snapshot:",
                "  - path: /api/v2/ads/get_total_balance",
                "    status: 403",
                "    fixture: ads_failure.json",
            ]
        ),
        encoding="utf-8",
    )

    failure_root = tmp_path / "failures"
    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env["FAILURE_ARTIFACTS_ROOT"] = str(failure_root)
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
        "--fixtures-dir",
        str(fixtures_dir),
        "--candidates",
        str(candidates_path),
        "--save-failure-artifacts",
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
    artifact_files = list(failure_root.rglob("*.json"))
    assert artifact_files, "expected failure artifacts"
    for artifact in artifact_files:
        content = artifact.read_text(encoding="utf-8")
        for marker in SECRET_MARKERS:
            assert marker not in content
