from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = REPO_ROOT / "tests" / "fixtures" / "shopee_ads"
PLAN_PATH = REPO_ROOT / "collaboration" / "plans" / "ads_ingest_minimal.yaml"
MAPPING_PATH = REPO_ROOT / "collaboration" / "mappings" / "ads_mapping.yaml"
TOKEN_FILE = (
    REPO_ROOT / "tests" / "fixtures" / "appsscript_tokens" / "shopee_tokens_export_example.json"
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


def _copy_fixture(name: str, dest: Path) -> None:
    dest.write_text(
        (FIXTURE_ROOT / name).read_text(encoding="utf-8"),
        encoding="utf-8",
    )


def test_ops_phase1_preview_failure_summary(tmp_path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)
    db_path = tmp_path / "preview_fail.db"
    reports_dir = tmp_path / "reports"

    fixtures_dir = tmp_path / "fixtures"
    fixtures_dir.mkdir()
    _copy_fixture("shop_info_ok_with_fake_secrets.json", fixtures_dir / "shop_info.json")
    _copy_fixture("ads_snapshot_ok_with_fake_secrets.json", fixtures_dir / "ads_snapshot.json")
    (fixtures_dir / "ads_daily.json").write_text(
        "\n".join(
            [
                "{",
                '  "error": "auth_failed",',
                '  "message": "denied",',
                '  "request_id": "req-789"',
                "}",
            ]
        ),
        encoding="utf-8",
    )

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
        "preview",
        "--date",
        "2026-02-03",
        "--only-shops",
        "samord,minmin",
        "--transport",
        "fixtures",
        "--token-file",
        str(TOKEN_FILE),
        "--fixtures-dir",
        str(fixtures_dir),
        "--plan",
        str(PLAN_PATH),
        "--mapping",
        str(MAPPING_PATH),
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
    assert "call_fail shop=samord call=ads_daily http=200" in stdout
    assert "api_error=auth_failed" in stdout
    assert "api_message=denied" in stdout
    assert "request_id=req-789" in stdout
