from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = REPO_ROOT / "tests" / "fixtures" / "shopee_ads"
PLAN_PATH = REPO_ROOT / "collaboration" / "plans" / "ads_probe_phase1.yaml"

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


def test_ops_phase1_ads_probe_fixtures(tmp_path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)
    artifacts_dir = tmp_path / "artifacts"
    analysis_dir = tmp_path / "analysis"

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env.pop("ALLOW_NETWORK", None)

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "ads",
        "probe",
        "--date",
        "2026-02-03",
        "--shops",
        "samord,minmin",
        "--transport",
        "fixtures",
        "--fixtures-dir",
        str(FIXTURE_ROOT),
        "--plan",
        str(PLAN_PATH),
        "--artifacts-dir",
        str(artifacts_dir),
        "--analysis-dir",
        str(analysis_dir),
        "--analyze",
        "--no-send-discord",
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
    assert "probe_ok=1" in stdout
    for marker in SECRET_MARKERS:
        assert marker not in stdout

    for shop in ["samord", "minmin"]:
        for call_name in ["shop_info", "ads_daily", "ads_snapshot"]:
            artifact_path = (
                artifacts_dir
                / shop
                / "2026-02-03"
                / "ads_probe"
                / f"{call_name}.json"
            )
            assert artifact_path.exists()
            content = artifact_path.read_text(encoding="utf-8")
            for marker in SECRET_MARKERS:
                assert marker not in content

        analysis_path = (
            analysis_dir / shop / "2026-02-03" / "ads_probe_summary.md"
        )
        assert analysis_path.exists()


def test_ops_phase1_ads_probe_live_requires_allow_network(tmp_path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)
    artifacts_dir = tmp_path / "artifacts"

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env.pop("ALLOW_NETWORK", None)

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "ads",
        "probe",
        "--date",
        "2026-02-03",
        "--shops",
        "samord,minmin",
        "--transport",
        "live",
        "--artifacts-dir",
        str(artifacts_dir),
    ]

    result = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert "network_disabled error=allow_network_required" in result.stdout
    if artifacts_dir.exists():
        assert not any(artifacts_dir.rglob("*"))
