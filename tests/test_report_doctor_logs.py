from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = REPO_ROOT / "tests" / "fixtures" / "shopee_ads"
MAPPING_PATH = REPO_ROOT / "collaboration" / "mappings" / "ads_mapping.yaml"
TOKEN_FILE = (
    REPO_ROOT / "tests" / "fixtures" / "appsscript_tokens" / "shopee_tokens_export_example.json"
)


def _write_shops(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "- shop_key: samord",
                "  label: SAMORD",
                "  enabled: true",
                "  shopee_shop_id: 497412318",
                "- shop_key: minmin",
                "  label: MINMIN",
                "  enabled: true",
                "  shopee_shop_id: 567655304",
            ]
        ),
        encoding="utf-8",
    )


def test_report_doctor_logs_emitted_in_schedule_run_once(tmp_path: Path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)
    db_path = tmp_path / "doctor.db"
    reports_dir = tmp_path / "reports"
    artifacts_root = tmp_path / "artifacts"

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
        "schedule",
        "run-once",
        "--job",
        "daily-midday",
        "--date",
        "2026-02-25",
        "--shops",
        "samord,minmin",
        "--transport",
        "fixtures",
        "--token-file",
        str(TOKEN_FILE),
        "--fixtures-dir",
        str(FIXTURE_ROOT),
        "--mapping",
        str(MAPPING_PATH),
        "--reports-dir",
        str(reports_dir),
        "--artifacts-root",
        str(artifacts_root),
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
    assert "report_doctor shop=samord kind=midday" in result.stdout
    assert "report_doctor shop=minmin kind=midday" in result.stdout
    assert "tables=" in result.stdout
    assert "text_len=" in result.stdout
    assert "style_tags=" in result.stdout
    assert "link_tags=" in result.stdout
    assert "meta_charset_ok=1" in result.stdout
