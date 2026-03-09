from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = REPO_ROOT / "tests" / "fixtures" / "shopee_ads"
PLAN_PATH = REPO_ROOT / "collaboration" / "plans" / "ads_ingest_minimal.yaml"
MAPPING_PATH = REPO_ROOT / "collaboration" / "mappings" / "ads_mapping.yaml"


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


def test_ops_phase1_run_all_fixtures(tmp_path: Path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)
    db_path = tmp_path / "run_all.db"
    reports_dir = tmp_path / "reports"
    artifacts_root = tmp_path / "artifacts"
    evidence_path = tmp_path / "evidence.md"
    failures_path = tmp_path / "failures.md"
    support_zip = tmp_path / "support_packet.zip"
    support_md = tmp_path / "support_request.md"

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env["DATABASE_URL"] = f"sqlite:///{db_path}"
    env.pop("ALLOW_NETWORK", None)
    env.pop("ADS_DAILY_PATH", None)
    env.pop("ADS_SNAPSHOT_PATH", None)

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "run-all",
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
        "--mapping",
        str(MAPPING_PATH),
        "--reports-dir",
        str(reports_dir),
        "--artifacts-root",
        str(artifacts_root),
        "--evidence-out",
        str(evidence_path),
        "--failures-out",
        str(failures_path),
        "--support-zip",
        str(support_zip),
        "--support-md",
        str(support_md),
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
    assert "phase1_run_all_start" in stdout
    assert "preflight_ok=1" in stdout
    assert "evidence_ok=1" in stdout
    assert "preview_ok=1" in stdout
    assert "phase1_run_all_ok=1" in stdout

    assert evidence_path.exists()
    assert failures_path.exists()
    assert support_zip.exists()
    assert support_md.exists()

    for shop in ["samord", "minmin"]:
        report_path = reports_dir / shop / "daily" / "2026-02-03_final.html"
        assert report_path.exists()
