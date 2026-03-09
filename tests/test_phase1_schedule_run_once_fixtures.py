from __future__ import annotations

import os
import sqlite3
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


def _count_rows(db_path: Path, table: str) -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.execute(f"SELECT COUNT(*) FROM {table}")
        row = cur.fetchone()
        return int(row[0] if row else 0)
    finally:
        conn.close()


def _run_schedule_once(
    *,
    env: dict[str, str],
    job: str,
    date_value: str,
    reports_dir: Path,
    artifacts_root: Path,
    plan_path: Path | None = None,
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
        job,
        "--date",
        date_value,
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
    if plan_path is not None:
        cmd.extend(["--plan", str(plan_path)])
    return subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_phase1_schedule_run_once_fixtures_daily_final(tmp_path: Path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)
    db_path = tmp_path / "daily_final.db"
    reports_dir = tmp_path / "reports"
    artifacts_root = tmp_path / "artifacts"

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env["DATABASE_URL"] = f"sqlite:///{db_path}"
    env.pop("ALLOW_NETWORK", None)
    env.pop("ADS_DAILY_PATH", None)
    env.pop("ADS_SNAPSHOT_PATH", None)

    # daily-final uses report date = anchor - 1 day.
    result = _run_schedule_once(
        env=env,
        job="daily-final",
        date_value="2026-02-04",
        reports_dir=reports_dir,
        artifacts_root=artifacts_root,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "phase1_schedule_run_once_ok=1" in result.stdout
    assert "planned_calls: shop_info, ads_daily" in result.stdout
    assert "planned_calls: shop_info, ads_daily, ads_snapshot" not in result.stdout

    assert _count_rows(db_path, "ads_campaign") > 0
    assert _count_rows(db_path, "ads_campaign_daily") > 0
    assert _count_rows(db_path, "ads_campaign_snapshot") == 0

    for shop in ["samord", "minmin"]:
        report_path = reports_dir / shop / "daily" / "2026-02-03_final.html"
        assert report_path.exists()


def test_phase1_schedule_run_once_fixtures_daily_midday(tmp_path: Path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)
    db_path = tmp_path / "daily_midday.db"
    reports_dir = tmp_path / "reports"
    artifacts_root = tmp_path / "artifacts"

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env["DATABASE_URL"] = f"sqlite:///{db_path}"
    env.pop("ALLOW_NETWORK", None)
    env.pop("ADS_DAILY_PATH", None)
    env.pop("ADS_SNAPSHOT_PATH", None)

    result = _run_schedule_once(
        env=env,
        job="daily-midday",
        date_value="2026-02-03",
        reports_dir=reports_dir,
        artifacts_root=artifacts_root,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "phase1_schedule_run_once_ok=1" in result.stdout
    assert "planned_calls: shop_info, ads_daily, ads_snapshot" in result.stdout

    assert _count_rows(db_path, "ads_campaign") > 0
    assert _count_rows(db_path, "ads_campaign_daily") > 0
    assert _count_rows(db_path, "ads_campaign_snapshot") > 0

    for shop in ["samord", "minmin"]:
        report_path = reports_dir / shop / "daily" / "2026-02-03_midday.html"
        assert report_path.exists()


def test_phase1_schedule_run_once_fixtures_weekly(tmp_path: Path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)
    db_path = tmp_path / "weekly.db"
    reports_dir = tmp_path / "reports"
    artifacts_root = tmp_path / "artifacts"

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env["DATABASE_URL"] = f"sqlite:///{db_path}"
    env.pop("ALLOW_NETWORK", None)
    env.pop("ADS_DAILY_PATH", None)
    env.pop("ADS_SNAPSHOT_PATH", None)

    result = _run_schedule_once(
        env=env,
        job="weekly",
        date_value="2026-02-10",
        reports_dir=reports_dir,
        artifacts_root=artifacts_root,
        plan_path=PLAN_PATH,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "phase1_schedule_run_once_ok=1" in result.stdout

    assert _count_rows(db_path, "ads_campaign") > 0
    assert _count_rows(db_path, "ads_campaign_daily") > 0
    assert _count_rows(db_path, "ads_campaign_snapshot") > 0

    for shop in ["samord", "minmin"]:
        report_path = reports_dir / shop / "weekly" / "2026-W06.html"
        assert report_path.exists()


def test_daily_final_does_not_increase_snapshot_rows_after_midday(tmp_path: Path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)
    db_path = tmp_path / "midday_then_final.db"
    reports_dir = tmp_path / "reports"
    artifacts_root = tmp_path / "artifacts"

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env["DATABASE_URL"] = f"sqlite:///{db_path}"
    env.pop("ALLOW_NETWORK", None)
    env.pop("ADS_DAILY_PATH", None)
    env.pop("ADS_SNAPSHOT_PATH", None)

    midday = _run_schedule_once(
        env=env,
        job="daily-midday",
        date_value="2026-02-19",
        reports_dir=reports_dir,
        artifacts_root=artifacts_root,
    )
    assert midday.returncode == 0, midday.stdout + midday.stderr
    assert "phase1_schedule_run_once_ok=1" in midday.stdout
    snapshot_before = _count_rows(db_path, "ads_campaign_snapshot")
    assert snapshot_before > 0

    final = _run_schedule_once(
        env=env,
        job="daily-final",
        date_value="2026-02-19",
        reports_dir=reports_dir,
        artifacts_root=artifacts_root,
    )
    assert final.returncode == 0, final.stdout + final.stderr
    assert "phase1_schedule_run_once_ok=1" in final.stdout
    assert "planned_calls: shop_info, ads_daily" in final.stdout
    assert "planned_calls: shop_info, ads_daily, ads_snapshot" not in final.stdout
    snapshot_after = _count_rows(db_path, "ads_campaign_snapshot")
    assert snapshot_after == snapshot_before


def test_daily_final_idempotent_on_same_date(tmp_path: Path) -> None:
    shops_path = tmp_path / "shops.yaml"
    _write_shops(shops_path)
    db_path = tmp_path / "daily_final_idempotent.db"
    reports_dir = tmp_path / "reports"
    artifacts_root = tmp_path / "artifacts"

    env = os.environ.copy()
    env["SHOPS_CONFIG_PATH"] = str(shops_path)
    env["DATABASE_URL"] = f"sqlite:///{db_path}"
    env.pop("ALLOW_NETWORK", None)
    env.pop("ADS_DAILY_PATH", None)
    env.pop("ADS_SNAPSHOT_PATH", None)

    first = _run_schedule_once(
        env=env,
        job="daily-final",
        date_value="2026-02-19",
        reports_dir=reports_dir,
        artifacts_root=artifacts_root,
    )
    assert first.returncode == 0, first.stdout + first.stderr
    assert "phase1_schedule_run_once_ok=1" in first.stdout

    counts_first = {
        "campaign": _count_rows(db_path, "ads_campaign"),
        "daily": _count_rows(db_path, "ads_campaign_daily"),
        "snapshot": _count_rows(db_path, "ads_campaign_snapshot"),
    }

    second = _run_schedule_once(
        env=env,
        job="daily-final",
        date_value="2026-02-19",
        reports_dir=reports_dir,
        artifacts_root=artifacts_root,
    )
    assert second.returncode == 0, second.stdout + second.stderr
    assert "phase1_schedule_run_once_ok=1" in second.stdout

    counts_second = {
        "campaign": _count_rows(db_path, "ads_campaign"),
        "daily": _count_rows(db_path, "ads_campaign_daily"),
        "snapshot": _count_rows(db_path, "ads_campaign_snapshot"),
    }
    assert counts_second == counts_first
