from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = REPO_ROOT / "tests" / "fixtures" / "shopee_ads"


def _write_shops(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "- shop_key: samord",
                "  label: SAMORD",
                "  enabled: true",
                "  shopee_shop_id: 111",
                "- shop_key: minmin",
                "  label: MINMIN",
                "  enabled: true",
                "  shopee_shop_id: 222",
            ]
        ),
        encoding="utf-8",
    )


def test_ops_smoke_ads_live_fixtures(tmp_path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)
    db_path = tmp_path / "smoke.db"
    reports_dir = tmp_path / "reports"

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env["DATABASE_URL"] = f"sqlite:///{db_path}"

    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "smoke",
        "ads-live-fixtures",
        "--date",
        "2026-02-03",
        "--only-shops",
        "samord,minmin",
        "--transport",
        "fixtures",
        "--fixtures-dir",
        str(FIXTURE_ROOT),
        "--plan",
        str(REPO_ROOT / "collaboration" / "plans" / "ads_ingest_minimal.yaml"),
        "--mapping",
        str(REPO_ROOT / "collaboration" / "mappings" / "ads_mapping.yaml"),
        "--reports-dir",
        str(reports_dir),
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
    assert "smoke_ok=1" in stdout
    assert "samord" in stdout
    assert "minmin" in stdout
    assert "planned_calls: shop_info, ads_campaign_list, ads_daily, ads_snapshot" in stdout
    assert "mapping_coverage: mapped=4 unmapped=0 missing=[]" in stdout
    assert "total calls_ok=8 calls_fail=0" in stdout
    assert "snapshots=4" in stdout
    assert "PARTNER_KEY_SHOULD_NOT_PRINT" not in stdout
    assert "ACCESS_TOKEN_SHOULD_NOT_PRINT" not in stdout
    assert "REFRESH_TOKEN_SHOULD_NOT_PRINT" not in stdout

    html_files = list(reports_dir.rglob("*.html"))
    assert len(html_files) == 2
