from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = REPO_ROOT / "tests" / "fixtures" / "shopee_ads"
TOKENS_DIR = REPO_ROOT / "tests" / "fixtures" / "appsscript_tokens"


def _write_shops(path: Path) -> None:
    lines = [
        "- shop_key: samord",
        "  label: SAMORD",
        "  enabled: true",
        "  shopee_shop_id: 497412318",
        "- shop_key: minmin",
        "  label: MINMIN",
        "  enabled: true",
        "  shopee_shop_id: 567655304",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_candidates(path: Path) -> None:
    path.write_text(
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


def test_sweep_skips_expired_shops(tmp_path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)
    candidates_path = tmp_path / "ads_candidates.yaml"
    _write_candidates(candidates_path)
    db_path = tmp_path / "sweep.db"

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env["DATABASE_URL"] = f"sqlite:///{db_path}"
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
        str(FIXTURE_ROOT),
        "--candidates",
        str(candidates_path),
        "--token-file",
        str(TOKENS_DIR / "shopee_tokens_export_mixed.json"),
        "--token-mode",
        "passive",
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
    assert "token_expired_refresh_disabled shop=samord" in stdout
    assert "shop=minmin kind=daily" in stdout
    assert "sweep_skipped_shops expired_access=samord" in stdout
    assert "sweep_ok=1" in stdout


def test_sweep_all_expired_exits_2(tmp_path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)
    candidates_path = tmp_path / "ads_candidates.yaml"
    _write_candidates(candidates_path)
    db_path = tmp_path / "sweep_all.db"

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env["DATABASE_URL"] = f"sqlite:///{db_path}"
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
        str(FIXTURE_ROOT),
        "--candidates",
        str(candidates_path),
        "--token-file",
        str(TOKENS_DIR / "shopee_tokens_export_expired.json"),
        "--token-mode",
        "passive",
    ]

    result = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2, result.stdout + result.stderr
    stdout = result.stdout
    assert "sweep_ok=0" in stdout
