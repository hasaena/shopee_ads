from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = REPO_ROOT / "tests" / "fixtures" / "shopee_ads"


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


def test_ops_phase1_ads_campaign_daily_truth_fixtures(tmp_path: Path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)
    artifacts_dir = tmp_path / "artifacts"

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "ads",
        "campaign-daily-truth",
        "--date",
        "2026-02-16",
        "--only-shops",
        "samord,minmin",
        "--fixtures-dir",
        str(FIXTURE_ROOT),
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
    assert result.returncode == 0, result.stdout + result.stderr
    stdout = result.stdout
    assert "campaign_daily_truth_start" in stdout
    assert "try_alt_endpoints=1" in stdout
    assert "campaign_daily_truth_endpoint shop=samord order=1 endpoint=get_product_campaign_daily_performance_with_id_list ok=1" in stdout
    assert "campaign_daily_truth_endpoint shop=minmin order=1 endpoint=get_product_campaign_daily_performance_with_id_list ok=1" in stdout
    assert "campaign_daily_truth_shop shop=samord verdict=SUPPORTED" in stdout
    assert "campaign_daily_truth_shop shop=minmin verdict=SUPPORTED" in stdout
    assert "campaign_daily_truth_ok=1" in stdout

    for shop in ["samord", "minmin"]:
        md = (
            artifacts_dir
            / shop
            / "2026-02-16"
            / "ads_campaign_daily_truth"
            / "ads_campaign_daily_truth_summary.md"
        )
        js = (
            artifacts_dir
            / shop
            / "2026-02-16"
            / "ads_campaign_daily_truth"
            / "ads_campaign_daily_truth_summary.json"
        )
        assert md.exists()
        assert js.exists()
        payload = json.loads(js.read_text(encoding="utf-8"))
        endpoint_results = payload.get("endpoint_results") or []
        assert len(endpoint_results) >= 1
        assert endpoint_results[0].get("endpoint") == "get_product_campaign_daily_performance_with_id_list"
        assert int(endpoint_results[0].get("ok") or 0) == 1


def test_ops_phase1_ads_campaign_daily_truth_live_requires_credentials(tmp_path: Path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env.pop("SHOPEE_PARTNER_ID", None)
    env.pop("SHOPEE_PARTNER_KEY", None)

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "ads",
        "campaign-daily-truth",
        "--date",
        "2026-02-16",
        "--only-shops",
        "samord,minmin",
        "--allow-network",
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
