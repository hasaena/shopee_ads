from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = REPO_ROOT / "tests" / "fixtures" / "shopee_ads"
PLAN_PATH = REPO_ROOT / "collaboration" / "plans" / "ads_probe_daily_truth.yaml"


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


def test_ops_phase1_ads_daily_truth_fixtures(tmp_path: Path) -> None:
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
        "daily-truth",
        "--date",
        "2026-02-25",
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
    assert "ads_daily_truth_start" in stdout
    assert "saved_json_path=" in stdout
    assert "daily_shape_detected shop=samord" in stdout
    assert "daily_shape_detected shop=minmin" in stdout
    assert "detected_fields shop=samord" in stdout
    assert "detected_fields shop=minmin" in stdout
    assert "ads_daily_truth_ok=1" in stdout

    for shop in ["samord", "minmin"]:
        daily_path = artifacts_dir / shop / "2026-02-25" / "ads_daily_truth" / "ads_daily.json"
        assert daily_path.exists()
        text = daily_path.read_text(encoding="utf-8")
        assert "ACCESS_TOKEN_SHOULD_NOT_PRINT" not in text
        summary_path = analysis_dir / shop / "2026-02-25" / "ads_daily_truth_summary.md"
        assert summary_path.exists()


def test_ops_phase1_ads_daily_truth_live_requires_allow_network(tmp_path: Path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)

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
        "daily-truth",
        "--date",
        "2026-02-25",
        "--shops",
        "samord,minmin",
        "--transport",
        "live",
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
