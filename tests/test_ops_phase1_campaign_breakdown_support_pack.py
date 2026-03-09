from __future__ import annotations

import os
import subprocess
import sys
import zipfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = REPO_ROOT / "tests" / "fixtures" / "shopee_ads"
TOKEN_FILE = (
    REPO_ROOT
    / "tests"
    / "fixtures"
    / "appsscript_tokens"
    / "shopee_tokens_export_example.json"
)


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


def test_ops_phase1_campaign_breakdown_support_pack_fixtures(tmp_path: Path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)
    db_path = tmp_path / "support_pack.db"
    artifacts_dir = tmp_path / "artifacts"
    out_dir = tmp_path / "support_packets"

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env["DATABASE_URL"] = f"sqlite:///{db_path}"

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "ads",
        "campaign-breakdown-support-pack",
        "--date",
        "2026-02-16",
        "--only-shops",
        "samord,minmin",
        "--transport",
        "fixtures",
        "--fixtures-dir",
        str(FIXTURE_ROOT),
        "--token-file",
        str(TOKEN_FILE),
        "--artifacts-dir",
        str(artifacts_dir),
        "--out-dir",
        str(out_dir),
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
    assert "support_pack_ok=1" in stdout

    zip_line = [line for line in stdout.splitlines() if line.startswith("zip_path=")]
    template_line = [
        line for line in stdout.splitlines() if line.startswith("ticket_template_path=")
    ]
    assert zip_line
    assert template_line
    zip_path = Path(zip_line[-1].split("=", 1)[1].strip())
    template_path = Path(template_line[-1].split("=", 1)[1].strip())
    assert zip_path.exists()
    assert template_path.exists()

    with zipfile.ZipFile(zip_path, "r") as zf:
        names = zf.namelist()
    assert "ticket_template.md" in names
    assert any(name.endswith("ads_campaign_daily_truth_summary.md") for name in names)
    assert any(name.endswith("ads_campaign_daily_truth_summary.json") for name in names)
