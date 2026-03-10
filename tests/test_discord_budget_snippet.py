from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = REPO_ROOT / "tests" / "fixtures" / "shopee_ads"
PLAN_PATH = REPO_ROOT / "collaboration" / "plans" / "ads_ingest_daily_final.yaml"
MAPPING_PATH = REPO_ROOT / "collaboration" / "mappings" / "ads_mapping.yaml"
TOKEN_FILE = (
    REPO_ROOT / "tests" / "fixtures" / "appsscript_tokens" / "shopee_tokens_export_example.json"
)


def _write_shops(path: Path, *, with_override: bool) -> None:
    lines = [
        "- shop_key: samord",
        "  label: SAMORD",
        "  enabled: true",
        "  shopee_shop_id: 497412318",
    ]
    if with_override:
        lines.append("  daily_budget_est: 300000")
    lines.extend(
        [
            "- shop_key: minmin",
            "  label: MINMIN",
            "  enabled: true",
            "  shopee_shop_id: 567655304",
        ]
    )
    if with_override:
        lines.append("  daily_budget_est: 200000")
    path.write_text("\n".join(lines), encoding="utf-8")


def _run_schedule_once(
    *,
    env: dict[str, str],
    reports_dir: Path,
    artifacts_root: Path,
) -> subprocess.CompletedProcess[str]:
    cmd = [
        sys.executable,
        "-m",
        "dotori_shopee_automation.cli",
        "ops",
        "phase1",
        "schedule",
        "run-once",
        "--job",
        "daily-final",
        "--date",
        "2026-02-16",
        "--shops",
        "samord,minmin",
        "--transport",
        "fixtures",
        "--token-file",
        str(TOKEN_FILE),
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
        "--no-send-discord",
    ]
    return subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_schedule_run_once_logs_budget_snippet_with_override_config_keeps_unknown_budget(
    tmp_path: Path,
) -> None:
    shops_path = tmp_path / "shops_override.yaml"
    _write_shops(shops_path, with_override=True)
    db_path = tmp_path / "override.db"
    reports_dir = tmp_path / "reports"
    artifacts_root = tmp_path / "artifacts"

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env["DATABASE_URL"] = f"sqlite:///{db_path}"

    result = _run_schedule_once(
        env=env,
        reports_dir=reports_dir,
        artifacts_root=artifacts_root,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "phase1_schedule_run_once_ok=1" in result.stdout
    assert (
        "discord_report_budget_snippet_disabled shop=samord kind=final "
        "reason=hidden_in_report budget_source=none"
    ) in result.stdout
    assert (
        "discord_report_budget_snippet_disabled shop=minmin kind=final "
        "reason=hidden_in_report budget_source=none"
    ) in result.stdout


def test_schedule_run_once_logs_budget_snippet_missing_without_budget(
    tmp_path: Path,
) -> None:
    shops_path = tmp_path / "shops_no_override.yaml"
    _write_shops(shops_path, with_override=False)
    db_path = tmp_path / "no_override.db"
    reports_dir = tmp_path / "reports"
    artifacts_root = tmp_path / "artifacts"

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env["DATABASE_URL"] = f"sqlite:///{db_path}"

    result = _run_schedule_once(
        env=env,
        reports_dir=reports_dir,
        artifacts_root=artifacts_root,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "phase1_schedule_run_once_ok=1" in result.stdout
    assert (
        "discord_report_budget_snippet_disabled shop=samord kind=final reason=hidden_in_report "
        "budget_source=none"
    ) in result.stdout
    assert (
        "discord_report_budget_snippet_disabled shop=minmin kind=final reason=hidden_in_report "
        "budget_source=none"
    ) in result.stdout
    assert "discord_report_budget_snippet shop=samord kind=final text=Budget:" not in result.stdout
    assert "discord_report_budget_snippet shop=minmin kind=final text=Budget:" not in result.stdout
